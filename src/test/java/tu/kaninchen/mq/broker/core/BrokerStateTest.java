package tu.kaninchen.mq.broker.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.core.exchange.Exchange;
import tu.kaninchen.mq.broker.core.exchange.ExchangeType;
import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.Optional;

class BrokerStateTest {

    private BrokerState brokerState;

    @BeforeEach
    public void init() {
        brokerState = new BrokerState();
    }

    @Test
    public void shouldAddQueueWhenQueueDoesNotExist() {
        // given
        Queue queue = someQueue();

        // when
        Optional<Queue> result = brokerState.addQueue(queue);

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertSame(queue, brokerState.getQueue("users").orElseThrow());
    }

    @Test
    public void shouldNotReplaceExistingQueueWhenAddingDuplicate() {
        // given
        Queue queue1 = someQueue();
        Queue queue2 = someQueue();

        // when
        Optional<Queue> result1 = brokerState.addQueue(queue1);
        Optional<Queue> result2 = brokerState.addQueue(queue2);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertSame(queue1, result2.get());
        Assertions.assertSame(queue1, brokerState.getQueue("users").orElseThrow());
    }

    @Test
    public void shouldAddMultipleDifferentQueues() {
        // given
        Queue queue1 = someQueue();
        Queue queue2 = new Queue("games");
        Queue queue3 = new Queue("movies");

        // when
        Optional<Queue> result1 = brokerState.addQueue(queue1);
        Optional<Queue> result2 = brokerState.addQueue(queue2);
        Optional<Queue> result3 = brokerState.addQueue(queue3);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isEmpty());
        Assertions.assertTrue(result3.isEmpty());
        Assertions.assertSame(queue1, brokerState.getQueue("users").orElseThrow());
        Assertions.assertSame(queue2, brokerState.getQueue("games").orElseThrow());
        Assertions.assertSame(queue3, brokerState.getQueue("movies").orElseThrow());
    }

    @Test
    public void shouldReturnQueueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);

        // then
        Assertions.assertSame(queue, brokerState.getQueue("users").orElseThrow());
    }

    @Test
    public void shouldReturnEmptyWhenQueueDoesNotExist() {
        // when & then
        Assertions.assertTrue(brokerState.getQueue("users").isEmpty());
    }

    @Test
    public void shouldRemoveQueueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);
        Optional<Queue> result = brokerState.removeQueue(queue.getName());

        // then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(queue, result.get());
        Assertions.assertTrue(brokerState.getQueue("users").isEmpty());
    }

    @Test
    public void shouldReturnEmptyWhenRemovingNonExistingQueue() {
        // given & when
        Optional<Queue> result = brokerState.removeQueue("users");

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(brokerState.getQueue("users").isEmpty());
    }

    @Test
    public void shouldReturnTrueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);

        // then
        Assertions.assertTrue(brokerState.containsQueue("users"));
    }

    @Test
    public void shouldReturnFalseWhenQueueDoesNotExist() {
        // when & then
        Assertions.assertFalse(brokerState.containsQueue("users"));
    }

    @Test
    public void shouldAddExchangeWhenExchangeDoesNotExist() {
        // given
        Exchange exchange = someExchange();

        // when
        Optional<Exchange> result = brokerState.addExchange(exchange);

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertSame(exchange, brokerState.getExchange("users").orElseThrow());
    }

    @Test
    public void shouldNotReplaceExistingExchangeWhenAddingDuplicate() {
        // given
        Exchange exchange1 = someExchange();
        Exchange exchange2 = someExchange();

        // when
        Optional<Exchange> result1 = brokerState.addExchange(exchange1);
        Optional<Exchange> result2 = brokerState.addExchange(exchange2);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertSame(exchange1, result2.get());
        Assertions.assertSame(exchange1, brokerState.getExchange("users").orElseThrow());
    }

    @Test
    public void shouldAddMultipleDifferentExchanges() {
        // given
        Exchange exchange1 = someExchange();
        Exchange exchange2 = new Exchange("games", ExchangeType.DIRECT);
        Exchange exchange3 = new Exchange("movies", ExchangeType.DIRECT);

        // when
        Optional<Exchange> result1 = brokerState.addExchange(exchange1);
        Optional<Exchange> result2 = brokerState.addExchange(exchange2);
        Optional<Exchange> result3 = brokerState.addExchange(exchange3);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isEmpty());
        Assertions.assertTrue(result3.isEmpty());
        Assertions.assertSame(exchange1, brokerState.getExchange("users").orElseThrow());
        Assertions.assertSame(exchange2, brokerState.getExchange("games").orElseThrow());
        Assertions.assertSame(exchange3, brokerState.getExchange("movies").orElseThrow());
    }

    @Test
    public void shouldReturnExchangeWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);

        // then
        Assertions.assertSame(exchange, brokerState.getExchange("users").orElseThrow());
    }

    @Test
    public void shouldReturnEmptyWhenExchangeDoesNotExist() {
        // when & then
        Assertions.assertTrue(brokerState.getExchange("users").isEmpty());
    }

    @Test
    public void shouldRemoveExchangeWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);
        Optional<Exchange> result = brokerState.removeExchange(exchange.getName());

        // then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(exchange, result.get());
        Assertions.assertTrue(brokerState.getExchange("users").isEmpty());
    }

    @Test
    public void shouldReturnEmptyWhenRemovingNonExistingExchange() {
        // given & when
        Optional<Exchange> result = brokerState.removeExchange("users");

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(brokerState.getExchange("users").isEmpty());
    }

    @Test
    public void shouldReturnTrueWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);

        // then
        Assertions.assertTrue(brokerState.containsExchange("users"));
    }

    @Test
    public void shouldReturnFalseWhenExchangeNotExist() {
        // when & then
        Assertions.assertFalse(brokerState.containsExchange("users"));
    }

    private Queue someQueue() {
        return new Queue("users");
    }

    private Exchange someExchange() {
        return new Exchange("users", ExchangeType.DIRECT);
    }
}