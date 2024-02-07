package it.polimi.coordinator;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    public Socket clientSocket;
    private String pathFile;
    private List<MutablePair<String, String>> operations;
    private boolean isPresentStep2;
    private volatile boolean keysProcessingCompleted;
    private boolean keysProcessingStarted;
    private boolean finalPhase;
    private Integer numWorkers;
    private List<Integer> keys;
    private Integer taskId;
    private KeyAssignmentManager keyManager;
    public SocketHandler(Socket clientSocket, String pathFile, List<MutablePair<String, String>> operations,boolean isPresentStep2, int numWorkers, KeyAssignmentManager keyManager,Integer taskId) {
        this.clientSocket = clientSocket;
        this.pathFile =pathFile; 
        this.operations = operations;
        this.isPresentStep2 = isPresentStep2;
        this.keysProcessingCompleted = false;
        this.keysProcessingStarted = false;
        this.numWorkers = numWorkers;
        this.keys = new ArrayList<>();
        this.keyManager = keyManager;
        this.taskId = taskId;
    }

    @Override
    public void run() {
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            
            Task t = new Task(operations, pathFile,isPresentStep2,taskId);
            outputStream.writeObject(t);

            while (true) {
                if(!keysProcessingStarted){
                    Object object = inputStream.readObject();
                    if (object == null) break;
                    
                    if (object instanceof List<?>) {
                        List<?> list = (List<?>) object;
                        // Process or print the list
                        if(!isPresentStep2){
                            System.out.println(list);
                        }else{
                            managePhase2(list);
                        }
                    } else if (object instanceof ErrorMessage) {
                        System.out.println("Not valid format operations file");
                    }
                }

                if(keysProcessingCompleted){
                    outputStream.writeObject(keys);
                    keysProcessingCompleted = false;
                    finalPhase = true;
                }

                if(finalPhase){
                    Object object = inputStream.readObject();
                    if (object == null) break;
                    
                    if (object instanceof List<?>) {
                        System.out.println(object);
                    } else if (object instanceof ErrorMessage) {
                        System.out.println("Something went wrong with the reduce phase!");
                    }
                    break;
                }

            }
            inputStream.close();
            outputStream.close();
            clientSocket.close();
        } catch (Exception e) {
            System.out.println("Worker connection lost");
        }
    }
    public void sendNewAssignment(List<Integer> newKeys) {
        this.keys = newKeys;
        this.keysProcessingCompleted = true;
    }
    
    public void setKeysProcessingStarted(boolean keysProcessingStarted) {
        this.keysProcessingStarted = keysProcessingStarted;
    }
    public void managePhase2(List<?> list){
        List<Integer> integerList = new ArrayList<>();
        for (Object element : list) {
            if (element instanceof Integer) {
                integerList.add((Integer) element);
            }
        }
        keyManager.insertAssignment(this, integerList,numWorkers);
        setKeysProcessingStarted(true);
    }    
}
