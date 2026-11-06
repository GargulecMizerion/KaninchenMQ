package tu.kaninchen.mq;

import tu.kaninchen.mq.server.BrokerServer;


public class Main {

    private static final int DEFAULT_PORT = 5680;

    static void main(String[] args) throws InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        BrokerServer server = new BrokerServer();
        server.start(port);

        // Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "shutdown-hook"));

        server.awaitTermination();
    }
}