package tu.kaninchen.mq.broker.api;

/** Treść jest buforem tylko do odczytu, także po powrocie z funkcji onDelivery. */
public record Delivery(long deliveryTag, byte[] body) {

    @Override
    public byte[] body() {
        return body.clone();
    }
}
