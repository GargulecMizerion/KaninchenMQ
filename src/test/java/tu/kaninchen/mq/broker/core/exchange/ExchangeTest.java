
package tu.kaninchen.mq.broker.core.exchange;

import org.junit.jupiter.api.Test;
import tu.kaninchen.mq.broker.core.queue.Queue;

import static org.junit.jupiter.api.Assertions.*;

class ExchangeTest {

    @Test
    public void shouldCreateExchangeWhenParametersAreValid() {
        // given
        String name = "users";
        ExchangeType type = ExchangeType.DIRECT;

        // when
        Exchange exchange = new Exchange(name, type);

        // then
        assertEquals(name, exchange.getName());
        assertEquals(type, exchange.getType());
        assertEquals(0, exchange.getBindingCount());
    }

    @Test
    public void shouldThrowExceptionWhenNameIsNull() {
        // given
        ExchangeType type = ExchangeType.DIRECT;

        // when & then
        assertThrows(
            NullPointerException.class,
            () -> new Exchange(null, type)
        );
    }

    @Test
    public void shouldThrowExceptionWhenTypeIsNull() {
        // given
        String name = "users";

        // when & then
        assertThrows(
            NullPointerException.class,
            () -> new Exchange(name, null)
        );
    }

    @Test
    public void shouldAddBinding() {
        // given
        Exchange exchange = exchange();
        Queue queue = queue("users");
        Binding binding = new Binding("users.created", queue);

        // when
        boolean result = exchange.addBinding(binding);

        // then
        assertTrue(result);
        assertEquals(1, exchange.getBindingCount());
    }

    @Test
    public void shouldNotAddDuplicateBinding() {
        // given
        Exchange exchange = exchange();
        Queue queue = queue("users");
        Binding binding = new Binding("users.created", queue);

        // when
        boolean firstResult = exchange.addBinding(binding);
        boolean secondResult = exchange.addBinding(binding);

        // then
        assertTrue(firstResult);
        assertFalse(secondResult);
        assertEquals(1, exchange.getBindingCount());
    }

    @Test
    public void shouldRemoveBinding() {
        // given
        Exchange exchange = exchange();
        Queue queue = queue("users");
        Binding binding = new Binding("users.created", queue);

        // when
        exchange.addBinding(binding);
        boolean result = exchange.removeBinding(binding);

        // then
        assertTrue(result);
        assertEquals(0, exchange.getBindingCount());
    }

    @Test
    public void shouldReturnFalseWhenRemovingNonExistingBinding() {
        // given
        Exchange exchange = exchange();
        Queue queue = queue("users");
        Binding binding = new Binding("users.created", queue);

        // when
        boolean result = exchange.removeBinding(binding);

        // then
        assertFalse(result);
        assertEquals(0, exchange.getBindingCount());
    }

    @Test
    public void shouldReturnCorrectBindingCount() {
        // given
        Exchange exchange = exchange();
        Queue usersQueue = queue("users");
        Queue ordersQueue = queue("orders");

        Binding usersBinding = new Binding("users.created", usersQueue);
        Binding ordersBinding = new Binding("orders.created", ordersQueue);

        // when
        exchange.addBinding(usersBinding);
        exchange.addBinding(ordersBinding);
        int countAfterAdding = exchange.getBindingCount();

        exchange.removeBinding(usersBinding);
        int countAfterRemoving = exchange.getBindingCount();

        // then
        assertEquals(2, countAfterAdding);
        assertEquals(1, countAfterRemoving);
    }

    private Exchange exchange() {
        return new Exchange("users", ExchangeType.DIRECT);
    }

    private Queue queue(String name) {
        return new Queue(name);
    }
}
