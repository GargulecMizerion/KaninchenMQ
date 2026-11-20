package tu.kaninchen.mq.broker.core.queue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;

public class Queue {

    private final String name;
    private final Deque<Message> messageDeque;

    public Queue(String name) {
        this.name = Objects.requireNonNull(name);
        this.messageDeque = new ArrayDeque<>();
    }

    public void enqueue(Message message) {
        messageDeque.add(message);
    }

    public Optional<Message> dequeue() {
        return Optional.ofNullable(messageDeque.pollFirst());
    }

    public void requeue(Message message) {
        messageDeque.addFirst(message);
    }

    public int size() {
        return messageDeque.size();
    }

    public boolean isEmpty() {
        return messageDeque.isEmpty();
    }

    public String getName() {
        return name;
    }
}
