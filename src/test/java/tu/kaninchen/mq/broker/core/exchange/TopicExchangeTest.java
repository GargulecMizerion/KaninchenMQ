package tu.kaninchen.mq.broker.core.exchange;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tu.kaninchen.mq.broker.core.queue.Queue;
import tu.kaninchen.mq.broker.model.BrokerResourceName;

import java.util.Set;

class TopicExchangeTest {

    @Test
    public void shouldReturnEmptySetWhenRoutingKeyDoesNotMatch() {
        // given
        Exchange exchange = new Exchange(new BrokerResourceName("exchange"), ExchangeType.TOPIC);
        Queue usersQueue = new Queue(new BrokerResourceName("users"));

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.deleted");

        // then
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void shouldReturnQueueWhenRoutingKeyMatchesExactly() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");

        // when
        exchange.addBinding(new Binding("users.created", usersQueue));
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "users.created",
        "users.updated",
        "users.deleted"
    })
    public void shouldMatchSingleSegmentWildcard(String routingKey) {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.*", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues(routingKey);

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "users",
        "users.created.email",
        "users.created.email.notification"
    })
    public void shouldNotMatchWhenSingleSegmentWildcardHasIncorrectNumberOfSegments(String routingKey) {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.*", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues(routingKey);

        // then
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void shouldMatchZeroSegmentsWithHashWildcard() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "users.created",
        "users.created.email",
        "users.created.email.notification"
    })
    public void shouldMatchOneOrMoreSegmentsWithHashWildcard(String routingKey) {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues(routingKey);

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchHashWildcardInTheMiddleOfBinding() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#.created", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.orders.payments.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldNotMatchWhenSegmentAfterHashDoesNotMatch() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#.created", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.orders.deleted");

        // then
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void shouldMatchZeroSegmentsWithHashWildcardInTheMiddle() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#.created", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchCombinedSingleAndMultipleSegmentWildcards() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.*.#", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created.email");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchMultipleAndSingleSegmentWildcards() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("users.#.*", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.orders.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchAllRoutingKeysWhenHashIsTheOnlyBindingSegment() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("#", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created.email");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchRoutingKeyWithHashWildcardAtTheBeginning() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("#.created", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.orders.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldMatchSingleSegmentWildcardAtTheBeginning() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        exchange.addBinding(new Binding("*.created", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldReturnQueueOnlyOnceWhenMultipleBindingsMatch() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");

        exchange.addBinding(new Binding("users.*", usersQueue));
        exchange.addBinding(new Binding("users.#", usersQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
    }

    @Test
    public void shouldReturnAllMatchingQueues() {
        // given
        Exchange exchange = topicExchange();
        Queue usersQueue = queue("users");
        Queue notificationsQueue = queue("notifications");

        exchange.addBinding(new Binding("users.*", usersQueue));
        exchange.addBinding(new Binding("users.#", notificationsQueue));

        // when
        Set<Queue> result = exchange.getMatchingQueues("users.created");

        // then
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.contains(usersQueue));
        Assertions.assertTrue(result.contains(notificationsQueue));
    }

    private Exchange topicExchange() {
        return new Exchange(new BrokerResourceName("users"), ExchangeType.TOPIC);
    }

    private Queue queue(String name) {
        return new Queue(new BrokerResourceName(name));
    }
}
