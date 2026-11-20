package tu.kaninchen.mq.server;

import tools.jackson.databind.JsonNode;
import tu.kaninchen.mq.broker.core.exchange.ExchangeType;

import java.util.Base64;

final class ClientCommand {
    private ClientCommand() {}

    static BrokerInterfaceDto decode(JsonNode frame, String requestId) {
        return switch (text(frame, "type")) {
            case "PING" -> new BrokerInterfaceDto.Ping(requestId);
            case "DECLARE_QUEUE" -> new BrokerInterfaceDto.DeclareQueue(requestId, name(frame, "queue"));
            case "DELETE_QUEUE" -> new BrokerInterfaceDto.DeleteQueue(requestId, name(frame, "queue"));
            case "DECLARE_EXCHANGE" -> new BrokerInterfaceDto.DeclareExchange(requestId, name(frame, "exchange"),
                    ExchangeType.valueOf(text(frame, "exchangeType")));
            case "DELETE_EXCHANGE" -> new BrokerInterfaceDto.DeleteExchange(requestId, name(frame, "exchange"));
            case "BIND" -> new BrokerInterfaceDto.Bind(requestId, name(frame, "exchange"), name(frame, "queue"),
                    text(frame, "bindingKey"));
            case "PUBLISH" -> new BrokerInterfaceDto.Publish(requestId, name(frame, "exchange"), text(frame, "routingKey"),
                    Base64.getDecoder().decode(text(frame, "bodyBase64")));
            case "SUBSCRIBE" -> new BrokerInterfaceDto.Subscribe(requestId, name(frame, "queue"), name(frame, "subscriptionId"),
                    prefetch(frame));
            case "ACK" -> new BrokerInterfaceDto.Ack(requestId, deliveryTag(frame));
            case "NACK" -> new BrokerInterfaceDto.Nack(requestId, deliveryTag(frame), requeue(frame));
            default -> throw new IllegalArgumentException("Unknown command");
        };
    }

    private static String text(JsonNode frame, String field) {
        JsonNode value = frame.path(field);
        if (!value.isString()) {
            throw new IllegalArgumentException(field + " must be a string");
        }
        return value.asString();
    }

    private static String name(JsonNode frame, String field) {
        String value = text(frame, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static int prefetch(JsonNode frame) {
        JsonNode value = frame.path("prefetch");
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new IllegalArgumentException("prefetch must be an integer");
        }
        return value.intValue();
    }

    private static long deliveryTag(JsonNode frame) {
        JsonNode value = frame.path("deliveryTag");
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IllegalArgumentException("deliveryTag must be an integer");
        }
        return value.longValue();
    }

    private static boolean requeue(JsonNode frame) {
        JsonNode value = frame.path("requeue");
        if (!value.isBoolean()) {
            throw new IllegalArgumentException("requeue must be a boolean");
        }
        return value.booleanValue();
    }
}
