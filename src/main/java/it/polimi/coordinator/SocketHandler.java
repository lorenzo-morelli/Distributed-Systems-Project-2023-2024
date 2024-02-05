package it.polimi.coordinator;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    private Socket clientSocket;
    private String pathFile;
    private  List<MutablePair<String, String>> operations;
    private boolean isPresentStep2;
    public SocketHandler(Socket clientSocket, String pathFile, List<MutablePair<String, String>> operations,boolean isPresentStep2) {
        this.clientSocket = clientSocket;
        this.pathFile =pathFile; 
        this.operations = operations;
        this.isPresentStep2 = isPresentStep2;
    }

    @Override
    public void run() {
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            
            Task t = new Task(operations, pathFile,isPresentStep2);
            outputStream.writeObject(t);

            while (true) {
                Object object = inputStream.readObject();
                if (object == null) break;
                
                if (object instanceof List<?>) {
                    List<?> list = (List<?>) object;
                    // Process or print the list
                    if(!isPresentStep2){
                        System.out.println(list);
                    }else{
                        System.out.println(list);
                    }
                } else if (object instanceof ErrorMessage) {
                    System.out.println("Not valid format operations file");
                }
            }
            inputStream.close();
            outputStream.close();
            clientSocket.close();
        } catch (Exception e) {
            System.out.println("Worker connection lost");
        }
    }
}
