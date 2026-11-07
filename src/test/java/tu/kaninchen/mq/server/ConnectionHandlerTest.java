package tu.kaninchen.mq.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionHandlerTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(new ConnectionHandler("c-test"));
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void echoesReceivedBytes() {
        channel.writeInbound(Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8));

        ByteBuf out = channel.readOutbound();
        assertNotNull(out, "handler should write the message back");
        try {
            assertEquals("hello", out.toString(StandardCharsets.UTF_8));
        } finally {
            out.release();
        }
    }

    @Test
    void echoesEachMessageInOrder() {
        channel.writeInbound(Unpooled.copiedBuffer("one", StandardCharsets.UTF_8));
        channel.writeInbound(Unpooled.copiedBuffer("two", StandardCharsets.UTF_8));

        assertEquals("one", readOutboundString());
        assertEquals("two", readOutboundString());
        assertNull(channel.readOutbound());
    }

    @Test
    void doesNotPassMessageFurtherDownThePipeline() {
        channel.writeInbound(Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8));
        readOutboundString();

        assertNull(channel.readInbound(), "message should be consumed by the handler");
    }

    @Test
    void closesChannelOnException() {
        assertTrue(channel.isActive());

        channel.pipeline().fireExceptionCaught(new RuntimeException("boom"));

        assertFalse(channel.isOpen(), "channel should be closed after an exception");
    }

    private String readOutboundString() {
        ByteBuf buf = channel.readOutbound();
        assertNotNull(buf);
        try {
            return buf.toString(StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }
}
