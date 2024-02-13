package it.polimi.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class Worker{
    
    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);

        System.out.println("Insert a port");
        String portString = scanner.nextLine();
        int port;
        try {
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
            while (true) {

                // Wait for a client to connect
                Socket clientSocket = serverSocket.accept();

                // Handle the connection in a separate thread
                WorkerHandler workerHandler = new WorkerHandler(clientSocket);
                workerHandler.start();
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
