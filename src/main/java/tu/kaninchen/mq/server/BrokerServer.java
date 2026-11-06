package tu.kaninchen.mq.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BrokerServer {

    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

    private final AtomicInteger connectionCounter = new AtomicInteger();
    private volatile EventLoopGroup bossGroup;
    private volatile EventLoopGroup workerGroup;
    private volatile Channel serverChannel;

    public synchronized int start(int port) throws InterruptedException {
        if (serverChannel != null) {
            throw new IllegalStateException("Server already started");
        }

        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            String connectionId = "c-" + connectionCounter.incrementAndGet();
                            ch.pipeline().addLast("connection", new ConnectionHandler(connectionId));
                        }
                    });

            serverChannel = bootstrap.bind(port).sync().channel();
        } catch (Exception e) {
            serverChannel = null;
            shutdownGroups();
            throw e;
        }

        int actualPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        log.info("KaninchenMQ listens on port: {}", actualPort);
        logConnectAddresses(actualPort);
        return actualPort;
    }

    private void logConnectAddresses(int port) {
        log.info("Connect with address:");
        log.info("  localhost:{}", port);
        try {
            for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback() || ni.isVirtual()) {
                    continue;
                }
                for (InetAddress addr : Collections.list(ni.getInetAddresses())) {
                    if (addr instanceof Inet4Address) {
                        log.info("  {}:{}  ({})", addr.getHostAddress(), port, ni.getDisplayName());
                    }
                }
            }
        } catch (SocketException e) {
            log.warn("Unable to read network interfaces", e);
        }
    }

    public synchronized void stop() {
        if (serverChannel == null) {
            return;
        }
        log.info("Server stopping...");
        serverChannel.close().syncUninterruptibly();
        serverChannel = null;
        shutdownGroups();
        log.info("Server closed");
    }

    public void awaitTermination() {
        Channel channel = serverChannel;
        if (channel != null) {
            channel.closeFuture().syncUninterruptibly();
        }
    }

    private void shutdownGroups() {
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            bossGroup = null;
        }
    }
}

