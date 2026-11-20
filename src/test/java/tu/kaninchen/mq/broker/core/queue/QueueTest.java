package tu.kaninchen.mq.broker.core.queue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class QueueTest {

    @Test
    public void shouldCreateQueueWhenNameIsValid() {
        // given
        String name = "users";

        // when
        Queue queue = new Queue(name);

        // then
        Assertions.assertEquals("users", queue.getName());
    }

    @Test
    public void shouldThrowExceptionWhenNameIsNull() {
        // when & then
        Assertions.assertThrows(NullPointerException.class, () -> new Queue(null));
    }

    @Test
    public void shouldAddMessageToQueue() {
        // given
        Message message = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message);

        // then
        Assertions.assertFalse(queue.isEmpty());
        Assertions.assertEquals(1, queue.size());
    }

    @Test
    public void shouldDequeueMessageCorrectly() {
        // given
        Message message = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message);
        Optional<Message> result = queue.dequeue();

        // then
        Assertions.assertTrue(result.isPresent());
        Assertions.assertSame(message, result.get());
        Assertions.assertTrue(queue.isEmpty());
        Assertions.assertEquals(0, queue.size());
    }

    @Test
    public void shouldPreserveFifoOrderWhenAddingMultipleMessages() {
        // given
        Message message1 = someMessage();
        Message message2 = someMessage();
        Message message3 = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message1);
        queue.enqueue(message2);
        queue.enqueue(message3);

        Optional<Message> result1 = queue.dequeue();
        Optional<Message> result2 = queue.dequeue();
        Optional<Message> result3 = queue.dequeue();

        // then
        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertTrue(result3.isPresent());
        Assertions.assertSame(message1, result1.get());
        Assertions.assertSame(message2, result2.get());
        Assertions.assertSame(message3, result3.get());
        Assertions.assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldReturnEmptyWhenQueueIsEmpty() {
        // given
        Queue queue = someQueue();

        // when
        Optional<Message> result = queue.dequeue();

        // then
        Assertions.assertTrue(result.isEmpty());
        Assertions.assertTrue(queue.isEmpty());
    }

    @Test
    public void shouldAddMessageToFrontWhenRequeueIsCalled() {
        // given
        Message message1 = someMessage();
        Message message2 = someMessage();
        Message message3 = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message1);
        queue.enqueue(message2);
        queue.requeue(message3);
        Optional<Message> result1 = queue.dequeue();
        Optional<Message> result2 = queue.dequeue();
        Optional<Message> result3 = queue.dequeue();

        // then
        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertTrue(result3.isPresent());
        Assertions.assertSame(message3, result1.get());
        Assertions.assertSame(message1, result2.get());
        Assertions.assertSame(message2, result3.get());
    }

    @Test
    public void shouldReturnCorrectSizeAfterAddingMessages() {
        // given
        Message message1 = someMessage();
        Message message2 = someMessage();
        Message message3 = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message1);
        queue.enqueue(message2);
        queue.requeue(message3);
        queue.dequeue();

        // then
        Assertions.assertFalse(queue.isEmpty());
        Assertions.assertEquals(2, queue.size());
    }

    @Test
    public void shouldReturnTrueWhenQueueIsEmpty() {
        // given
        Queue queue = someQueue();

        // when & then
        Assertions.assertTrue(queue.isEmpty());
        Assertions.assertEquals(0 , queue.size());
    }

    @Test
    public void shouldReturnFalseWhenQueueIsNotEmpty() {
        // given
        Message message = someMessage();
        Queue queue = someQueue();

        // when
        queue.enqueue(message);

        // then
        Assertions.assertFalse(queue.isEmpty());
    }

    private Queue someQueue() {
        return new Queue("users");
    }

    private Message someMessage() {
        return new Message(1, new byte[] {1, 2, 3});
    }
}