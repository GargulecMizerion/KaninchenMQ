package tu.kaninchen.mq.broker.core.exchange;

import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class DirectExchangeTest {

    @Test
    public void shouldReturnQueueWhenRoutingKeyMatchesExactly() {
        // given
        Exchange exchange = directExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        assertEquals(Set.of(usersQueue), result);
    }

    @Test
    public void shouldReturnEmptySetWhenRoutingKeyDoesNotMatch() {
        // given
        Exchange exchange = directExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.deleted");

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldReturnEmptySetWhenExchangeHasNoBindings() {
        // given
        Exchange exchange = directExchange();

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldReturnMultipleQueuesWhenRoutingKeyMatchesMultipleBindings() {
        // given
        Exchange exchange = directExchange();
        Queue usersQueue = queue("users");
        Queue auditQueue = queue("audit");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        exchange.addBinding(new Binding("users.created", auditQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        assertEquals(Set.of(usersQueue, auditQueue), result);
    }

    @Test
    public void shouldReturnQueueOnlyOnceWhenMultipleBindingsMatch() {
        // given
        Exchange exchange = directExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        assertEquals(1, result.size());
        assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldNotMatchDifferentRoutingKeysWithSimilarSegments() {
        // given
        Exchange exchange = directExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.created.email");

        // then
        assertTrue(result.isEmpty());
    }

    private Exchange directExchange() {
        return new Exchange("users", ExchangeType.DIRECT);
    }

    private Queue queue(String name) {
        return new Queue(name);
    }
}

