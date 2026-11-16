package tu.kaninchen.mq.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class BrokerServerTest {

    private BrokerServer server;

    @BeforeEach
    void setUp() {
        server = new BrokerServer();
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    void startOnPortZeroReturnsAssignedPort() throws Exception {
        int port = server.start(0);

        assertTrue(port > 0, "OS should assign a real port");
    }

    @Test
    void answersPingOverTcp() throws Exception {
        int port = server.start(0);

        try (Socket socket = connect(port)) {
            assertPong(socket, "hej");
        }
    }

    @Test
    void handlesMultipleMessagesOnSameConnection() throws Exception {
        int port = server.start(0);

        try (Socket socket = connect(port)) {
            assertPong(socket, "first");
            assertPong(socket, "second");
        }
    }

    @Test
    void handlesMultipleClientsConcurrently() throws Exception {
        int port = server.start(0);
        int clients = 10;

        List<CompletableFuture<Void>> results = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            String requestId = "client-" + i;
            results.add(CompletableFuture.runAsync(() -> {
                try (Socket socket = connect(port)) {
                    assertPong(socket, requestId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (CompletableFuture<Void> result : results) {
            result.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void startingTwiceThrows() throws Exception {
        server.start(0);

        assertThrows(IllegalStateException.class, () -> server.start(0));
    }

    @Test
    void stopClosesThePort() throws Exception {
        int port = server.start(0);

        server.stop();

        assertThrows(ConnectException.class, () -> connect(port).close());
    }

    @Test
    void stopIsIdempotentAndSafeBeforeStart() {
        assertDoesNotThrow(() -> {
            server.stop();
            server.stop();
        });
    }

    @Test
    void canBeRestartedAfterStop() throws Exception {
        server.start(0);
        server.stop();

        int port = server.start(0);

        try (Socket socket = connect(port)) {
            assertPong(socket, "again");
        }
    }

    @Test
    void failedBindLeavesServerReusable() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0)) {
            int busyPort = occupied.getLocalPort();

            assertThrows(BindException.class, () -> server.start(busyPort));
        }

        int port = server.start(0);
        assertTrue(port > 0, "server should start normally after a failed bind");
    }

    @Test
    void awaitTerminationReturnsAfterStop() throws Exception {
        server.start(0);

        CompletableFuture<Void> waiting = CompletableFuture.runAsync(server::awaitTermination);
        assertFalse(waiting.isDone(), "awaitTermination should block while server runs");

        server.stop();

        waiting.get(5, TimeUnit.SECONDS);
    }

    @Test
    void awaitTerminationReturnsImmediatelyWhenNotStarted() {
        assertDoesNotThrow(server::awaitTermination);
    }

    private static Socket connect(int port) throws IOException {
        Socket socket = new Socket("localhost", port);
        socket.setSoTimeout(3000);
        return socket;
    }

    private static void assertPong(Socket socket, String requestId) throws IOException {
        String ping = "{\"type\":\"PING\",\"requestId\":\"%s\"}\n".formatted(requestId);
        String pong = "{\"type\":\"PONG\",\"requestId\":\"%s\"}\n".formatted(requestId);
        OutputStream out = socket.getOutputStream();
        out.write(ping.getBytes(StandardCharsets.UTF_8));
        out.flush();

        byte[] response = socket.getInputStream().readNBytes(pong.getBytes(StandardCharsets.UTF_8).length);
        assertEquals(pong, new String(response, StandardCharsets.UTF_8));
    }
}
