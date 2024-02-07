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
    private Integer numWorkers;
    private List<Integer> keys;
    private Integer taskId;
    private KeyAssignmentManager keyManager;
    private CoordinatorPhase phase;
    private volatile boolean canProceedStep2;

    public SocketHandler(Socket clientSocket, String pathFile, List<MutablePair<String, String>> operations,boolean isPresentStep2, int numWorkers, KeyAssignmentManager keyManager,Integer taskId) {
        this.clientSocket = clientSocket;
        this.pathFile = pathFile; 
        this.operations = operations;
        this.isPresentStep2 = isPresentStep2;
        this.numWorkers = numWorkers;
        this.keyManager = keyManager;
        this.taskId = taskId;
        this.phase = CoordinatorPhase.INIT;
        this.keys = new ArrayList<>();
        this.canProceedStep2 = false;
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

            boolean isProcessing = true;
            while (isProcessing) {
                switch (phase) {
                    case INIT:
                        Object object = inputStream.readObject();
                        if (object == null){ 
                            isProcessing = false;
                            break;
                        }
                        if (object instanceof List<?>) {
                            List<?> list = (List<?>) object;
                            // Process or print the list
                            if (!isPresentStep2) {
                                System.out.println(list);
                            } else {
                                managePhase2(list);
                            }
                        } else if (object instanceof ErrorMessage) {
                            System.out.println("Not valid format operations file");
                        }
                        break;
            
                    case KEYPROCESS:
                        if(canProceedStep2){
                            outputStream.writeObject(keys);
                            phase = CoordinatorPhase.FINAL;
                        }
                        break;
            
                    case FINAL:
                        Object finalObject = inputStream.readObject();
                        isProcessing = false;
                        if (finalObject == null) {
                            break;
                        }            
                        if (finalObject instanceof List<?>) {
                            System.out.println(finalObject);
                        } else if (finalObject instanceof ErrorMessage) {
                            System.out.println("Something went wrong with the reduce phase!");
                        }
                        break;
                    default:
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
        this.canProceedStep2 = true;
    }

    public void managePhase2(List<?> list){
        List<Integer> integerList = new ArrayList<>();
        for (Object element : list) {
            if (element instanceof Integer) {
                integerList.add((Integer) element);
            }
        }
        keyManager.insertAssignment(this, integerList,numWorkers);
        this.phase = CoordinatorPhase.KEYPROCESS;
    }    
}
