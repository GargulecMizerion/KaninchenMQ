package tu.kaninchen.mq.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.api.Broker;
import tu.kaninchen.mq.broker.api.BrokerException;
import tu.kaninchen.mq.broker.api.Delivery;
import tu.kaninchen.mq.broker.api.ExchangeType;
import tu.kaninchen.mq.broker.api.PublishResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionHandlerTest {

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void waitsForFullFrameAndSplitsMultipleFramesFromOneRead() {
        openChannel(null);
        receive("{\"type\":\"PING\",");
        assertNull(channel.readOutbound());

        receive("""
                "requestId":"first"}
                {"type":"PING","requestId":"second"}
                """);

        assertReply("{\"type\":\"PONG\",\"requestId\":\"first\"}");
        assertReply("{\"type\":\"PONG\",\"requestId\":\"second\"}");
        assertNull(channel.readOutbound());
    }

    @Test
    void reportsBadRequestsAndKeepsReadingTheConnection() {
        openChannel(null);
        receive("""
                invalid JSON
                {"type":"PING"}
                {"type":"UNKNOWN","requestId":"1"}
                {"type":"PUBLISH","requestId":"2","exchange":"orders","routingKey":"","bodyBase64":"!"}
                {"type":"PING","requestId":"3"} {"type":"PING","requestId":"4"}
                {"type":"PING","requestId":"5"}
                """);

        assertReply("{\"type\":\"ERROR\",\"requestId\":null,\"code\":\"BAD_REQUEST\"}");
        assertReply("{\"type\":\"ERROR\",\"requestId\":null,\"code\":\"BAD_REQUEST\"}");
        assertReply("{\"type\":\"ERROR\",\"requestId\":\"1\",\"code\":\"BAD_REQUEST\"}");
        assertReply("{\"type\":\"ERROR\",\"requestId\":\"2\",\"code\":\"BAD_REQUEST\"}");
        assertReply("{\"type\":\"ERROR\",\"requestId\":null,\"code\":\"BAD_REQUEST\"}");
        assertReply("{\"type\":\"PONG\",\"requestId\":\"5\"}");
    }

    @Test
    void reportsUnavailableBrokerForValidBrokerCommand() {
        openChannel(null);
        receive("""
                {"type":"DECLARE_QUEUE","requestId":"q1","queue":"orders.created"}
                """);

        assertReply("{\"type\":\"ERROR\",\"requestId\":\"q1\",\"code\":\"BROKER_UNAVAILABLE\"}");
    }

    @Test
    void dispatchesTopologyCommands() {
        RecordingBroker broker = new RecordingBroker();
        openChannel(broker);
        receive("""
                {"type":"DECLARE_QUEUE","requestId":"q1","queue":"orders.created"}
                {"type":"DECLARE_EXCHANGE","requestId":"e1","exchange":"orders","exchangeType":"DIRECT"}
                {"type":"BIND","requestId":"b1","exchange":"orders","queue":"orders.created","bindingKey":"order.created"}
                {"type":"DELETE_QUEUE","requestId":"q2","queue":"orders.created"}
                {"type":"DELETE_EXCHANGE","requestId":"e2","exchange":"orders"}
                """);

        assertEquals(List.of(
                List.of("declareQueue", "orders.created"),
                List.of("declareExchange", "orders", ExchangeType.DIRECT),
                List.of("bind", "orders", "orders.created", "order.created"),
                List.of("deleteQueue", "orders.created"),
                List.of("deleteExchange", "orders")
        ), broker.calls);
        for (String requestId : List.of("q1", "e1", "b1", "q2", "e2")) {
            assertReply("{\"type\":\"OK\",\"requestId\":\"%s\"}".formatted(requestId));
        }
    }

    @Test
    void readsOtherCommandsWhilePublicationIsPending() {
        RecordingBroker broker = new RecordingBroker();
        openChannel(broker);
        receive("""
                {"type":"PUBLISH","requestId":"p1","exchange":"orders","routingKey":"order.created","bodyBase64":"AP8="}
                {"type":"PING","requestId":"ping1"}
                """);

        assertEquals(List.of(List.of("publish", "orders", "order.created")), broker.calls);
        assertArrayEquals(new byte[]{0, (byte) 255}, broker.publishedBody);
        assertReply("{\"type\":\"PONG\",\"requestId\":\"ping1\"}");
        assertNull(channel.readOutbound());

        broker.publication.complete(new PublishResult(2));
        channel.runPendingTasks();
        assertReply("{\"type\":\"OK\",\"requestId\":\"p1\",\"routedQueues\":2}");
    }

    @Test
    void mapsDomainErrorsToResponses() {
        RecordingBroker broker = new RecordingBroker();
        openChannel(broker);
        receive("""
                {"type":"PUBLISH","requestId":"p1","exchange":"orders","routingKey":"","bodyBase64":""}
                """);

        broker.publication.completeExceptionally(new BrokerException(BrokerException.Code.EXCHANGE_NOT_FOUND));
        channel.runPendingTasks();
        assertReply("{\"type\":\"ERROR\",\"requestId\":\"p1\",\"code\":\"EXCHANGE_NOT_FOUND\"}");
    }

    @Test
    void deliversMessagesAndPassesAcknowledgementsWithTheSessionId() {
        RecordingBroker broker = new RecordingBroker();
        openChannel(broker);
        receive("""
                {"type":"SUBSCRIBE","requestId":"s1","queue":"orders.created","subscriptionId":"reader","prefetch":1}
                """);
        assertReply("{\"type\":\"OK\",\"requestId\":\"s1\"}");

        broker.onDelivery.accept(new Delivery(17, new byte[]{0, (byte) 255}));
        channel.runPendingTasks();
        assertReply("{\"type\":\"DELIVERY\",\"subscriptionId\":\"reader\",\"deliveryTag\":17,\"bodyBase64\":\"AP8=\"}");

        receive("""
                {"type":"ACK","requestId":"a1","deliveryTag":17}
                {"type":"NACK","requestId":"n1","deliveryTag":18,"requeue":true}
                """);
        assertReply("{\"type\":\"OK\",\"requestId\":\"a1\"}");
        assertReply("{\"type\":\"OK\",\"requestId\":\"n1\"}");
        channel.close();

        assertEquals(List.of(
                List.of("subscribe", "c-test", "reader", "orders.created", 1),
                List.of("ack", "c-test", 17L),
                List.of("nack", "c-test", 18L, true),
                List.of("closeSession", "c-test")
        ), broker.calls);
    }

    @Test
    void closesConnectionOnOversizedFrameWithoutWaitingForNewline() {
        openChannel(null);
        int size = BrokerServer.MAX_FRAME_BYTES + 1;
        channel.writeInbound(Unpooled.buffer(size).writeZero(size));

        assertFalse(channel.isOpen());
    }

    private void openChannel(Broker broker) {
        channel = new EmbeddedChannel();
        BrokerServer.configurePipeline(channel.pipeline(), "c-test", broker);
    }

    private void receive(String input) {
        channel.writeInbound(Unpooled.copiedBuffer(input, StandardCharsets.UTF_8));
        channel.runPendingTasks();
    }

    private void assertReply(String expected) {
        ByteBuf reply = channel.readOutbound();
        assertNotNull(reply);
        try {
            assertEquals(expected + "\n", reply.toString(StandardCharsets.UTF_8));
        } finally {
            reply.release();
        }
    }

    private static final class RecordingBroker implements Broker {
        final List<List<Object>> calls = new ArrayList<>();
        final CompletableFuture<PublishResult> publication = new CompletableFuture<>();
        byte[] publishedBody;
        Consumer<Delivery> onDelivery;

        @Override
        public CompletionStage<Void> declareQueue(String queue) {
            return recordCall("declareQueue", queue);
        }

        @Override
        public CompletionStage<Void> deleteQueue(String queue) {
            return recordCall("deleteQueue", queue);
        }

        @Override
        public CompletionStage<Void> declareExchange(String exchange, ExchangeType type) {
            return recordCall("declareExchange", exchange, type);
        }

        @Override
        public CompletionStage<Void> deleteExchange(String exchange) {
            return recordCall("deleteExchange", exchange);
        }

        @Override
        public CompletionStage<Void> bind(String exchange, String queue, String bindingKey) {
            return recordCall("bind", exchange, queue, bindingKey);
        }

        @Override
        public CompletionStage<PublishResult> publish(String exchange, String routingKey, byte[] body) {
            calls.add(List.of("publish", exchange, routingKey));
            publishedBody = body;
            return publication;
        }

        @Override
        public CompletionStage<Void> subscribe(
                String sessionId, String subscriptionId, String queue, int prefetch, Consumer<Delivery> onDelivery) {
            this.onDelivery = onDelivery;
            return recordCall("subscribe", sessionId, subscriptionId, queue, prefetch);
        }

        @Override
        public CompletionStage<Void> ack(String sessionId, long deliveryTag) {
            return recordCall("ack", sessionId, deliveryTag);
        }

        @Override
        public CompletionStage<Void> nack(String sessionId, long deliveryTag, boolean requeue) {
            return recordCall("nack", sessionId, deliveryTag, requeue);
        }

        @Override
        public CompletionStage<Void> closeSession(String sessionId) {
            return recordCall("closeSession", sessionId);
        }

        private CompletionStage<Void> recordCall(Object... call) {
            calls.add(List.of(call));
            return CompletableFuture.completedFuture(null);
        }
    }
}
