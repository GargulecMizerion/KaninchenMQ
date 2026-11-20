package tu.kaninchen.mq.server;

import tu.kaninchen.mq.broker.core.exchange.ExchangeType;

/** Polecenia klienta po zdekodowaniu ramki JSON przez serwer TCP. */
public sealed interface BrokerInterfaceDto {
    String requestId();

    record Ping(String requestId) implements BrokerInterfaceDto {}

    record DeclareQueue(String requestId, String queue) implements BrokerInterfaceDto {}

    record DeleteQueue(String requestId, String queue) implements BrokerInterfaceDto {}

    record DeclareExchange(String requestId, String exchange, ExchangeType exchangeType)
            implements BrokerInterfaceDto {}

    record DeleteExchange(String requestId, String exchange) implements BrokerInterfaceDto {}

    record Bind(String requestId, String exchange, String queue, String bindingKey)
            implements BrokerInterfaceDto {}

    record Publish(String requestId, String exchange, String routingKey, byte[] body)
            implements BrokerInterfaceDto {}

    record Subscribe(String requestId, String queue, String subscriptionId, int prefetch)
            implements BrokerInterfaceDto {}

    record Ack(String requestId, long deliveryTag) implements BrokerInterfaceDto {}

    record Nack(String requestId, long deliveryTag, boolean requeue) implements BrokerInterfaceDto {}
}
