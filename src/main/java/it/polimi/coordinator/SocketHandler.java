package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    private Socket clientSocket;

    public SocketHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        // Create input and output streams for communication
        ObjectInputStream inputStream;
        ObjectOutputStream outputStream;

        try {
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());

            while (true) {
                Object object = inputStream.readObject();

                if (object == null) break;
                if (object instanceof List<?>) {
                    List<?> list = (List<?>) object;
                    // Process or print the list
                    System.out.println(list);
                } else if (object instanceof ErrorMessage) {
                    System.out.println("Not valid format operations file");
                }
            }
            inputStream.close();
            outputStream.close();
            clientSocket.close();
        } catch (Exception e) {
            e.getStackTrace();
        }
//        finally {
//            try {
//                // Close the streams and socket when done
//                if (inputStream != null) {
//                    inputStream.close();
//                }
//                if (outputStream != null) {
//                    outputStream.close();
//                }
//                if (clientSocket != null && !clientSocket.isClosed()) {
//                    clientSocket.close();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
