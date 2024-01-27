package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    private Socket clientSocket;
    private BlockingQueue<Task> taskQueue;
    public SocketHandler(Socket clientSocket,BlockingQueue<Task> taskQueue) {
        this.clientSocket = clientSocket;
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
        
            while (true) {
                Task task = taskQueue.poll(); // Retrieves and removes the head of the queue, returns null if empty
                if (task == null) {
                    // No task available, exit the loop
                    break;
                }

                // Send data to the server using outputStream
                // Receive data from the server using inputStream
                outputStream.writeObject(task);
                System.out.println("Sent task to server");

                Object object = inputStream.readObject();
                System.out.println(object);
            }
            inputStream.close();
            outputStream.close();
            if(clientSocket.isConnected()){
                clientSocket.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
                try {
                    // Close the streams and socket when done
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
