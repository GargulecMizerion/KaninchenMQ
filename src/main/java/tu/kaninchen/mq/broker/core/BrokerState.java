package tu.kaninchen.mq.broker.core;

import tu.kaninchen.mq.broker.core.exchange.Exchange;
import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BrokerState {

    private final Map<String, Queue> queues;
    private final Map<String, Exchange> exchanges;

    public BrokerState() {
        this.queues = new HashMap<>();
        this.exchanges = new HashMap<>();
    }

    public Optional<Queue> addQueue(Queue queue) {
        return Optional.ofNullable(queues.putIfAbsent(queue.getName(), queue));
    }

    public Optional<Queue> getQueue(String name) {
        return Optional.ofNullable(queues.get(name));
    }

    public Optional<Queue> removeQueue(String name) {
        return Optional.ofNullable(queues.remove(name));
    }

    public boolean containsQueue(String name) {
        return queues.containsKey(name);
    }

    public Optional<Exchange> addExchange(Exchange exchange) {
        return Optional.ofNullable(exchanges.putIfAbsent(exchange.getName(), exchange));
    }

    public Optional<Exchange> getExchange(String name) {
        return Optional.ofNullable(exchanges.get(name));
    }

    public Optional<Exchange> removeExchange(String name) {
        return Optional.ofNullable(exchanges.remove(name));
    }

    public boolean containsExchange(String name) {
        return exchanges.containsKey(name);
    }
}
