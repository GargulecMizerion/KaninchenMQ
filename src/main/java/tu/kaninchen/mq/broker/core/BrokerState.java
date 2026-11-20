package tu.kaninchen.mq.broker.core;

import tu.kaninchen.mq.broker.core.exchange.Exchange;
import tu.kaninchen.mq.broker.core.queue.Queue;
import tu.kaninchen.mq.broker.model.BrokerResourceName;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BrokerState {

    private final Map<BrokerResourceName, Queue> queues;
    private final Map<BrokerResourceName, Exchange> exchanges;

    public BrokerState() {
        this.queues = new HashMap<>();
        this.exchanges = new HashMap<>();
    }

    public Optional<Queue> addQueue(Queue queue) {
        return Optional.ofNullable(queues.putIfAbsent(queue.getResourceName(), queue));
    }

    public Optional<Queue> getQueue(BrokerResourceName name) {
        return Optional.ofNullable(queues.get(name));
    }

    public Optional<Queue> removeQueue(BrokerResourceName name) {
        return Optional.ofNullable(queues.remove(name));
    }

    public boolean containsQueue(BrokerResourceName name) {
        return queues.containsKey(name);
    }

    public Optional<Exchange> addExchange(Exchange exchange) {
        return Optional.ofNullable(exchanges.putIfAbsent(exchange.getResourceName(), exchange));
    }

    public Optional<Exchange> getExchange(BrokerResourceName name) {
        return Optional.ofNullable(exchanges.get(name));
    }

    public Optional<Exchange> removeExchange(BrokerResourceName name) {
        return Optional.ofNullable(exchanges.remove(name));
    }

    public boolean containsExchange(BrokerResourceName name) {
        return exchanges.containsKey(name);
    }
}
