package tu.kaninchen.mq.broker.core.queue;

import java.util.Objects;

public record Message(long id, byte[] body) {

    public Message {
        Objects.requireNonNull(body);
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
