package tu.kaninchen.mq.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ConnectionHandler.class);

    private final String connectionId;

    public ConnectionHandler(String connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("[{}] Nowe połączenie z {}", connectionId, ctx.channel().remoteAddress());
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ctx.writeAndFlush(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("[{}] Rozłączono", connectionId);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[{}] Błąd, zamykam połączenie: {}", connectionId, cause.toString());
        ctx.close();
    }
}
