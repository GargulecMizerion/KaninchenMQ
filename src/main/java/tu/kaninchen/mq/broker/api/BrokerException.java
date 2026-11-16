package tu.kaninchen.mq.broker.api;

public final class BrokerException extends RuntimeException {
    public enum Code {
        QUEUE_NOT_FOUND,
        QUEUE_IN_USE,
        EXCHANGE_NOT_FOUND,
        EXCHANGE_TYPE_MISMATCH,
        SUBSCRIPTION_EXISTS,
        INVALID_PREFETCH,
        DELIVERY_NOT_FOUND
    }

    private final Code code;

    public BrokerException(Code code) {
        super(code.name());
        this.code = code;
    }

    public Code code() {
        return code;
    }
}
