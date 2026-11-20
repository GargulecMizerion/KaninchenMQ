package tu.kaninchen.mq.broker.core.exchange;

import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.core.queue.Queue;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FanoutExchangeTest {

    @Test
    public void shouldReturnAllQueuesRegardlessOfRoutingKey() {
        // given
        Exchange exchange = fanoutExchange();
        Queue usersQueue = queue("users");
        Queue ordersQueue = queue("orders");
        Queue notificationsQueue = queue("notifications");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        exchange.addBinding(new Binding("orders.created", ordersQueue));
        exchange.addBinding(new Binding("notifications.created", notificationsQueue));
        Set<Queue> result = exchange.getMatchingQueues("anything");

        // then
        assertEquals(Set.of(usersQueue, ordersQueue, notificationsQueue), result);
    }

    @Test
    public void shouldReturnEmptySetWhenExchangeHasNoBindings() {
        // given
        Exchange exchange = fanoutExchange();

        // when
        Set<Queue> result = exchange.getMatchingQueues("anything");

        // then
        assertTrue(result.isEmpty());
    }

    @Test
    public void shouldReturnSingleQueueWhenOnlyOneBindingExists() {
        // given
        Exchange exchange = fanoutExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("anything");

        // then
        assertEquals(Set.of(usersQueue), result);
    }

    @Test
    public void shouldReturnQueueOnlyOnceWhenMultipleBindingsPointToSameQueue() {
        // given
        Exchange exchange = fanoutExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        exchange.addBinding(new Binding("users.updated", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("anything");

        // then
        assertEquals(1, result.size());
        assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldReturnAllQueuesRegardlessOfBindingKeys() {
        // given
        Exchange exchange = fanoutExchange();
        Queue usersQueue = queue("users");
        Queue ordersQueue = queue("orders");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        exchange.addBinding(new Binding("orders.deleted", ordersQueue));
        Set<Queue> result = exchange.getMatchingQueues("completely.different.key");

        // then
        assertEquals(Set.of(usersQueue, ordersQueue), result);
    }

    private Exchange fanoutExchange() {
        return new Exchange("notifications", ExchangeType.FANOUT);
    }

    private Queue queue(String name) {
        return new Queue(name);
    }
}

