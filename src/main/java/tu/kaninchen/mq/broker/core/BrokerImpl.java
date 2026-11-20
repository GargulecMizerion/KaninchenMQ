package tu.kaninchen.mq.broker.core;

import tu.kaninchen.mq.broker.api.Broker;
import tu.kaninchen.mq.broker.api.Delivery;
import tu.kaninchen.mq.broker.api.PublishResult;
import tu.kaninchen.mq.broker.core.exchange.ExchangeType;
import tu.kaninchen.mq.broker.core.queue.Queue;
import tu.kaninchen.mq.broker.model.BrokerResourceName;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public class BrokerImpl implements Broker {

    private final BrokerState brokerState;

    public BrokerImpl(BrokerState brokerState) {
        this.brokerState = brokerState;
    }

    @Override
    public CompletionStage<Void> declareQueue(String queueName) {
//        try {
//            Queue queue = new Queue(new BrokerResourceName(queueName));
//        } catch () {
//
//        }
        return null;
    }

    @Override
    public CompletionStage<Void> deleteQueue(String queueName) {
        return null;
    }

    @Override
    public CompletionStage<Void> declareExchange(String exchangeName, ExchangeType type) {
        return null;
    }

    @Override
    public CompletionStage<Void> deleteExchange(String exchangeName) {
        return null;
    }

    @Override
    public CompletionStage<Void> bind(String exchangeName, String queueName, String bindingKey) {
        return null;
    }

    @Override
    public CompletionStage<PublishResult> publish(String exchangeName, String routingKey, byte[] body) {
        return null;
    }

    @Override
    public CompletionStage<Void> subscribe(String sessionId, String subscriptionId, String queueName,
                                           int prefetch, Consumer<Delivery> onDelivery) {
        return null;
    }

    @Override
    public CompletionStage<Void> ack(String sessionId, long deliveryTag) {
        return null;
    }

    @Override
    public CompletionStage<Void> nack(String sessionId, long deliveryTag, boolean requeue) {
        return null;
    }

    @Override
    public CompletionStage<Void> closeSession(String sessionId) {
        return null;
    }
}
