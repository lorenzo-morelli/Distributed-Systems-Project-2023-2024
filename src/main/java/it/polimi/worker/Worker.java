package it.polimi.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import org.apache.log4j.LogManager;

public class Worker{

        private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
        public static void main(String[] args){
        
        
        PropertyConfigurator.configure("conf/log4j.properties");
        

        Scanner scanner = new Scanner(System.in);
        
        String address;
        int port;
        try {

            System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
            address = scanner.nextLine();
            address = (address.equals("")) ? "hdfs://localhost:9000" : "hdfs://" + address;


            System.out.println("Insert a port");
            String portString = scanner.nextLine();
            port = Integer.parseInt(portString);
            scanner.close();
        } catch (Exception e) {
            System.out.println("Not a number input port");
            scanner.close();
            return;
        }
        ServerSocket serverSocket = null;
        try {
            // Create a server socket to accept connections
            serverSocket = new ServerSocket(port);
            System.out.println("Server started on port " + port);
            logger.info("Server started on port " + port);
            while (true) {

                // Wait for a client to connect
                Socket clientSocket = serverSocket.accept();


                logger.info("Coordinator opened a connection.");

                // Handle the connection in a separate thread
                WorkerHandler workerHandler = new WorkerHandler(clientSocket, new HadoopWorker(address));
                workerHandler.start();
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
