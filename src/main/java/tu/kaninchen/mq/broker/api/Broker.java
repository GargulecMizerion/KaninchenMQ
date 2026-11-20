package tu.kaninchen.mq.broker.api;

import tu.kaninchen.mq.broker.core.exchange.ExchangeType;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Asynchroniczne operacje rdzenia. Metody mogą być wywoływane równolegle z wielu
 * połączeń i zwracają wynik przyszłej operacji przed rozpoczęciem pracy blokującej.
 */
public interface Broker {
    CompletionStage<Void> declareQueue(String queue);
    CompletionStage<Void> deleteQueue(String queue);
    CompletionStage<Void> declareExchange(String exchange, ExchangeType type);
    CompletionStage<Void> deleteExchange(String exchange);
    CompletionStage<Void> bind(String exchange, String queue, String bindingKey);

    CompletionStage<PublishResult> publish(String exchange, String routingKey, byte[] body);

    CompletionStage<Void> subscribe(
            String sessionId, String subscriptionId, String queue,
            int prefetch, Consumer<Delivery> onDelivery);

    CompletionStage<Void> ack(String sessionId, long deliveryTag);
    CompletionStage<Void> nack(String sessionId, long deliveryTag, boolean requeue);
    CompletionStage<Void> closeSession(String sessionId);
}
