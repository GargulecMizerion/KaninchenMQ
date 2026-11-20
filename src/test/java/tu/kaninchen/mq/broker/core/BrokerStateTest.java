package tu.kaninchen.mq.broker.core;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.core.exchange.Exchange;
import tu.kaninchen.mq.broker.core.exchange.ExchangeType;
import tu.kaninchen.mq.broker.core.queue.Queue;
import tu.kaninchen.mq.broker.model.BrokerResourceName;

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
        Assertions.assertSame(queue, brokerState.getQueue(new BrokerResourceName("users")).orElseThrow());
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
        Assertions.assertSame(queue1, brokerState.getQueue(new BrokerResourceName("users")).orElseThrow());
    }

    @Test
    public void shouldAddMultipleDifferentQueues() {
        // given
        Queue queue1 = someQueue();
        Queue queue2 = new Queue(new BrokerResourceName("games"));
        Queue queue3 = new Queue(new BrokerResourceName("movies"));

        // when
        Optional<Queue> result1 = brokerState.addQueue(queue1);
        Optional<Queue> result2 = brokerState.addQueue(queue2);
        Optional<Queue> result3 = brokerState.addQueue(queue3);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isEmpty());
        Assertions.assertTrue(result3.isEmpty());
        Assertions.assertSame(queue1, brokerState.getQueue(new BrokerResourceName("users")).orElseThrow());
        Assertions.assertSame(queue2, brokerState.getQueue(new BrokerResourceName("games")).orElseThrow());
        Assertions.assertSame(queue3, brokerState.getQueue(new BrokerResourceName("movies")).orElseThrow());
    }

    @Test
    public void shouldReturnQueueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);

        // then
        Assertions.assertSame(queue, brokerState.getQueue(new BrokerResourceName("users")).orElseThrow());
    }

    @Test
    public void shouldReturnEmptyWhenQueueDoesNotExist() {
        // when & then
        Assertions.assertTrue(brokerState.getQueue(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldRemoveQueueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);
        Optional<Queue> result = brokerState.removeQueue(queue.getResourceName());

        // then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(queue, result.get());
        Assertions.assertTrue(brokerState.getQueue(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldReturnEmptyWhenRemovingNonExistingQueue() {
        // given & when
        Optional<Queue> result = brokerState.removeQueue(new BrokerResourceName("users"));

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(brokerState.getQueue(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldReturnTrueWhenQueueExists() {
        // given
        Queue queue = someQueue();

        // when
        brokerState.addQueue(queue);

        // then
        Assertions.assertTrue(brokerState.containsQueue(new BrokerResourceName("users")));
    }

    @Test
    public void shouldReturnFalseWhenQueueDoesNotExist() {
        // when & then
        Assertions.assertFalse(brokerState.containsQueue(new BrokerResourceName("users")));
    }

    @Test
    public void shouldAddExchangeWhenExchangeDoesNotExist() {
        // given
        Exchange exchange = someExchange();

        // when
        Optional<Exchange> result = brokerState.addExchange(exchange);

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertSame(exchange, brokerState.getExchange(new BrokerResourceName("users")).orElseThrow());
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
        Assertions.assertSame(exchange1, brokerState.getExchange(new BrokerResourceName("users")).orElseThrow());
    }

    @Test
    public void shouldAddMultipleDifferentExchanges() {
        // given
        Exchange exchange1 = someExchange();
        Exchange exchange2 = new Exchange(new BrokerResourceName("games"), ExchangeType.DIRECT);
        Exchange exchange3 = new Exchange(new BrokerResourceName("movies"), ExchangeType.DIRECT);

        // when
        Optional<Exchange> result1 = brokerState.addExchange(exchange1);
        Optional<Exchange> result2 = brokerState.addExchange(exchange2);
        Optional<Exchange> result3 = brokerState.addExchange(exchange3);

        // then
        Assertions.assertTrue(result1.isEmpty());
        Assertions.assertTrue(result2.isEmpty());
        Assertions.assertTrue(result3.isEmpty());
        Assertions.assertSame(exchange1, brokerState.getExchange(new BrokerResourceName("users")).orElseThrow());
        Assertions.assertSame(exchange2, brokerState.getExchange(new BrokerResourceName("games")).orElseThrow());
        Assertions.assertSame(exchange3, brokerState.getExchange(new BrokerResourceName("movies")).orElseThrow());
    }

    @Test
    public void shouldReturnExchangeWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);

        // then
        Assertions.assertSame(exchange, brokerState.getExchange(new BrokerResourceName("users")).orElseThrow());
    }

    @Test
    public void shouldReturnEmptyWhenExchangeDoesNotExist() {
        // when & then
        Assertions.assertTrue(brokerState.getExchange(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldRemoveExchangeWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);
        Optional<Exchange> result = brokerState.removeExchange(exchange.getResourceName());

        // then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(exchange, result.get());
        Assertions.assertTrue(brokerState.getExchange(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldReturnEmptyWhenRemovingNonExistingExchange() {
        // given & when
        Optional<Exchange> result = brokerState.removeExchange(new BrokerResourceName("users"));

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(brokerState.getExchange(new BrokerResourceName("users")).isEmpty());
    }

    @Test
    public void shouldReturnTrueWhenExchangeExists() {
        // given
        Exchange exchange = someExchange();

        // when
        brokerState.addExchange(exchange);

        // then
        Assertions.assertTrue(brokerState.containsExchange(new BrokerResourceName("users")));
    }

    @Test
    public void shouldReturnFalseWhenExchangeNotExist() {
        // when & then
        Assertions.assertFalse(brokerState.containsExchange(new BrokerResourceName("users")));
    }

    private Queue someQueue() {
        return new Queue(new BrokerResourceName("users"));
    }

    private Exchange someExchange() {
        return new Exchange(new BrokerResourceName("users"), ExchangeType.DIRECT);
    }
}