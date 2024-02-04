package it.polimi.worker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Worker extends Thread {
    private String hostname;
    private int port;
    private ServerSocket serverSocket;
    private boolean isRunning = true;

    public Worker(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public boolean getIsRunning(){
        return isRunning;
    }

    @Override
    public void run() {            
        try {
            
            // Create a server socket to accept connections
            serverSocket = new ServerSocket(port);
            
            while (isRunning) {

                // Wait for a client to connect
                Socket clientSocket = serverSocket.accept();

                // Handle the connection in a separate thread
                WorkerHandler workerHandler = new WorkerHandler(clientSocket);
                workerHandler.start();
            }

            // Close the server socket when the thread is interrupted
            serverSocket.close();
        } catch (IOException e) {
            if (!serverSocket.isClosed()) {
                e.printStackTrace();
            }
        }
    }

    // Method to gracefully stop the server
    public void stopServer() {
        isRunning = false; // Interrupt the thread
        try {
            serverSocket.close(); // Close the server socket to unblock accept
        } catch (IOException e) {
            System.out.println("Unable to stop server");
        }
    }
}
