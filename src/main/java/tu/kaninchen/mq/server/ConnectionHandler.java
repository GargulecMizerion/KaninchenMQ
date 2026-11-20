package tu.kaninchen.mq.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import tu.kaninchen.mq.broker.api.Broker;
import tu.kaninchen.mq.broker.exception.BrokerException;
import tu.kaninchen.mq.broker.api.Delivery;
import tu.kaninchen.mq.broker.api.PublishResult;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;

public class ConnectionHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
            .build();

    private final String connectionId;
    private final Broker broker;

    ConnectionHandler(String connectionId, Broker broker) {
        this.connectionId = connectionId;
        this.broker = broker;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("[{}] Nowe połączenie z {}", connectionId, ctx.channel().remoteAddress());
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        JsonNode request;
        try {
            String json = StandardCharsets.UTF_8.newDecoder().decode(frame.nioBuffer()).toString();
            request = JSON.readTree(json);
        } catch (CharacterCodingException | JacksonException e) {
            writeError(ctx, null, "BAD_REQUEST");
            return;
        }

        JsonNode id = request.path("requestId");
        if (!id.isString() || id.asString().isBlank()) {
            writeError(ctx, null, "BAD_REQUEST");
            return;
        }
        String requestId = id.asString();

        BrokerInterfaceDto command;
        try {
            command = ClientCommand.decode(request, requestId);
        } catch (IllegalArgumentException e) {
            writeError(ctx, requestId, "BAD_REQUEST");
            return;
        }

        if (command instanceof BrokerInterfaceDto.Ping) {
            write(ctx, response("PONG", requestId));
            return;
        }
        if (broker == null) {
            writeError(ctx, requestId, "BROKER_UNAVAILABLE");
            return;
        }

        try {
            complete(ctx, command.requestId(), invokeBroker(ctx, command));
        } catch (RuntimeException e) {
            writeBrokerError(ctx, requestId, e);
        }
    }

    private CompletionStage<?> invokeBroker(ChannelHandlerContext ctx, BrokerInterfaceDto command) {
        return switch (command) {
            case BrokerInterfaceDto.DeclareQueue cmd -> broker.declareQueue(cmd.queue());
            case BrokerInterfaceDto.DeleteQueue cmd -> broker.deleteQueue(cmd.queue());
            case BrokerInterfaceDto.DeclareExchange cmd -> broker.declareExchange(cmd.exchange(), cmd.exchangeType());
            case BrokerInterfaceDto.DeleteExchange cmd -> broker.deleteExchange(cmd.exchange());
            case BrokerInterfaceDto.Bind cmd -> broker.bind(cmd.exchange(), cmd.queue(), cmd.bindingKey());
            case BrokerInterfaceDto.Publish cmd -> broker.publish(cmd.exchange(), cmd.routingKey(), cmd.body());
            case BrokerInterfaceDto.Subscribe cmd -> broker.subscribe(
                    connectionId, cmd.subscriptionId(), cmd.queue(), cmd.prefetch(),
                    delivery -> sendDelivery(ctx, cmd.subscriptionId(), delivery));
            case BrokerInterfaceDto.Ack cmd -> broker.ack(connectionId, cmd.deliveryTag());
            case BrokerInterfaceDto.Nack cmd -> broker.nack(connectionId, cmd.deliveryTag(), cmd.requeue());
            case BrokerInterfaceDto.Ping ignored -> throw new IllegalStateException("PING is handled by the server");
        };
    }

    private void complete(ChannelHandlerContext ctx, String requestId, CompletionStage<?> operation) {
        operation.whenCompleteAsync((result, error) -> {
            if (error != null) {
                writeBrokerError(ctx, requestId, error);
                return;
            }
            ObjectNode reply = response("OK", requestId);
            if (result instanceof PublishResult published) {
                reply.put("routedQueues", published.routedQueues());
            }
            write(ctx, reply);
        }, ctx.executor());
    }

    private void sendDelivery(ChannelHandlerContext ctx, String subscriptionId, Delivery delivery) {
        try {
            ctx.executor().execute(() -> {
                ObjectNode frame = JSON.createObjectNode()
                        .put("type", "DELIVERY")
                        .put("subscriptionId", subscriptionId)
                        .put("deliveryTag", delivery.deliveryTag())
                        .put("bodyBase64", Base64.getEncoder().encodeToString(delivery.body()));
                write(ctx, frame);
            });
        } catch (RejectedExecutionException ignored) {
            // Podczas zamykania połączenia channelInactive zleca odzyskanie dostarczeń.
        }
    }

    private void writeBrokerError(ChannelHandlerContext ctx, String requestId, Throwable error) {
        while (error instanceof CompletionException completion && completion.getCause() != null) {
            error = completion.getCause();
        }
        if (error instanceof BrokerException rejected) {
            writeError(ctx, requestId, rejected.code().name());
            return;
        }
        log.error("[{}] Błąd podczas obsługi polecenia", connectionId, error);
        writeError(ctx, requestId, "INTERNAL_ERROR");
    }

    private static void writeError(ChannelHandlerContext ctx, String requestId, String code) {
        write(ctx, response("ERROR", requestId).put("code", code));
    }

    private static ObjectNode response(String type, String requestId) {
        return JSON.createObjectNode().put("type", type).put("requestId", requestId);
    }

    private static void write(ChannelHandlerContext ctx, ObjectNode frame) {
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(JSON.writeValueAsString(frame) + "\n")
                    .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("[{}] Rozłączono", connectionId);
        if (broker != null) {
            try {
                broker.closeSession(connectionId).whenComplete((ignored, error) -> {
                    if (error != null) {
                        log.error("[{}] Błąd podczas zamykania sesji", connectionId, error);
                    }
                });
            } catch (RuntimeException e) {
                log.error("[{}] Błąd podczas zamykania sesji", connectionId, e);
            }
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[{}] Błąd, zamykam połączenie: {}", connectionId, cause.toString());
        ctx.close();
    }
}
