package tu.kaninchen.mq.broker.core.exchange;

import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.Objects;

public record Binding(String bindingKey, Queue queue) {
    public Binding {
        Objects.requireNonNull(bindingKey);
        Objects.requireNonNull(queue);
    }
}
