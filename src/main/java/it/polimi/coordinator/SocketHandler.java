package it.polimi.coordinator;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import it.polimi.common.Messages.Task;

public class SocketHandler implements Runnable {
    private Socket clientSocket;
    private Task task;
    public SocketHandler(Socket clientSocket, Task task) {
        this.clientSocket = clientSocket;
        this.task = task;
    }

    @Override
    public void run() {
        try (
            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream())
        ) {
            // Send data to the server using outputStream
            // Receive data from the server using inputStream
            outputStream.writeObject(task);
            System.out.println("I send something");

            Object object = inputStream.readObject();

            System.out.println(object);

            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
