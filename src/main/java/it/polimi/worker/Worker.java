package it.polimi.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import org.apache.log4j.LogManager;

/**
 * This class is responsible for starting the worker server.
 * The worker server listens for connections from the coordinator.
 * When a connection is established, the worker server creates a WorkerHandler thread to handle the connection.
 */
public class Worker {

    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");

    /**
     * Main method to start the worker server.
     *
     * @param args Command line arguments (not used).
     */
    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        Scanner scanner = new Scanner(System.in);
        String address;
        int port;
        try {
            System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
            address = scanner.nextLine();
            address = (address.isEmpty()) ? "hdfs://localhost:9000" : "hdfs://" + address;

        } catch (Exception e) {
            System.out.println("Not a valid address");
            scanner.close();
            return;
        }
        try {
            System.out.println("Insert a port");
            String portString = scanner.nextLine();
            port = Integer.parseInt(portString);
            scanner.close();
        } catch (Exception e) {
            System.out.println("Not a number input port");
            scanner.close();
            return;
        }
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started on port " + port);
            logger.info("Server started on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                logger.info("Coordinator opened a connection.");
                WorkerHandler workerHandler = new WorkerHandler(clientSocket, new HadoopWorker(address));
                workerHandler.start();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
