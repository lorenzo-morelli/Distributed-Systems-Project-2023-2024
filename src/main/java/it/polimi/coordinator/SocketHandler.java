package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;

import it.polimi.common.Address;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.LastReduce;
import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    public Socket clientSocket;
    private Integer taskId;
    private Coordinator coordinator;
    private String file;
    private KeyAssignmentManager keyManager;
    private CoordinatorPhase phase;
    private ObjectInputStream inputStream = null;
    private ObjectOutputStream outputStream = null;
    private boolean isProcessing;
    public SocketHandler(Coordinator coordinator, String file, Integer taskId,CoordinatorPhase phase) {
        this.clientSocket = coordinator.getFileSocketMap().get(file);
        this.keyManager = coordinator.getKeyManager();
        this.file = file;
        this.taskId = taskId;
        this.coordinator = coordinator;
        this.phase = phase;
        this.isProcessing = true;
    }

    @Override
    public void run() {
        try {
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            
            while (isProcessing) {
                switch (phase) {
                    case INIT:
                        Task t = new Task(coordinator.getOperations(), file,coordinator.checkChangeKeyReduce(),taskId);
                        System.out.println("Sending task to worker phase1");
                        outputStream.writeObject(t);
                        Object object = inputStream.readObject();
                        if (object == null){ 
                            System.out.println("Received a null object!");
                            System.exit(0);
                        }else if (object instanceof ErrorMessage) {
                            System.out.println(((ErrorMessage) object).getMessage());
                            System.exit(0);
                        }else if (object instanceof List<?>) {
                            List<?> list = (List<?>) object;
                            // Process or print the list
                            if (!coordinator.checkChangeKeyReduce()) {
                                System.out.println(list);
                                end(object);
                                isProcessing = false;
                            } else {
                                managePhase2(list);
                            }
                        } 
                        break;
            
                    case FINAL:
                        if(keyManager.canProceed()){
                            
                            System.out.println("Sending task to worker phase2");
                            LastReduce lastReduce = new LastReduce(coordinator.getLastReduce(), keyManager.getFinalAssignments().get(this));
                            outputStream.writeObject(lastReduce);
        
                        
                            Object finalObject = inputStream.readObject();
                            if (finalObject == null ) {
                                System.out.println("Something went wrong with the reduce phase!");
                                System.exit(0);
                            }else if (finalObject instanceof ErrorMessage){
                                System.out.println(((ErrorMessage) finalObject).getMessage());
                                System.exit(0);
                            }else if (finalObject instanceof List<?>) {
                                System.out.println(finalObject);
                                end(finalObject);
                            }            
                            isProcessing = false;
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
            handleSocketException();
        }
    }
    private void end(Object object){
        try{
            coordinator.writeResult(convertObjectToListKeyValuePairs(object));
        }catch(Exception e){
            System.out.println("Error while writing the final result");
            System.out.println(e.getMessage());
        }
    }

    private List<KeyValuePair> convertObjectToListKeyValuePairs(Object object) {
        List<?> objectList = (List<?>) object;
        List<KeyValuePair> list = new ArrayList<>();   
        
        for (Object element : objectList) {
            if (element instanceof KeyValuePair) {
                list.add((KeyValuePair) element);
            }
        }
        return list;
    }

    private void managePhase2(List<?> list){
        List<Integer> integerList = new ArrayList<>();
        for (Object element : list) {
            if (element instanceof Integer) {
                integerList.add((Integer) element);
            }
        }
        keyManager.insertAssignment(this, integerList,coordinator.getNumPartitions());
        this.phase = CoordinatorPhase.FINAL;
    }
    private void handleSocketException() {
      
      
        coordinator.getClientSockets().remove(clientSocket);
        coordinator.getFileSocketMap().put(file, null);
    
        if (isProcessing) {
            boolean reconnected = attemptReconnection(3, 5000);
    
            if (!reconnected) {
                System.out.println("Not possible to reconnect to the failed worker. Assigning to another worker...");
                reconnected = attemptReconnectionFromPool(3, 5000);
    
                if (!reconnected) {
                    System.out.println("Not possible to connect to any worker.");
                    System.exit(0);
                } else {
                    performReconnectedActions();
                }
            } else {
                performReconnectedActions();
            }
        }
    }
    
    private boolean attemptReconnection(int maxAttempts, long reconnectDelayMillis) {
        boolean reconnected = false;
        int attempts = 0;
    
        while (!reconnected && attempts < maxAttempts) {
            try {
                clientSocket = new Socket(clientSocket.getInetAddress().getHostName(), clientSocket.getPort());
                reconnected = true;
            } catch (IOException e) {
                attempts++;
                System.out.println("Reconnection attempt " + attempts + " failed. Retrying...");
    
                try {
                    Thread.sleep(reconnectDelayMillis);
                } catch (InterruptedException interruptedException) {
                    System.out.println("Reconnection attempt interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    
        return reconnected;
    }
    
    private boolean attemptReconnectionFromPool(int maxAttempts, long reconnectDelayMillis) {
        boolean reconnected = false;
        int attempts = 0;
    
        while (!reconnected && attempts < maxAttempts) {
            try {
                List<Address> addresses = new ArrayList<>(coordinator.getAddresses());
                addresses.remove(new Address(clientSocket.getInetAddress().getHostName(), clientSocket.getPort()));
                clientSocket = coordinator.getNewActiveSocket(addresses);
                reconnected = true;
            } catch (Exception e) {
                attempts++;
                System.out.println("Reconnection attempt " + attempts + " failed. Retrying...");
    
                try {
                    Thread.sleep(reconnectDelayMillis);
                } catch (InterruptedException interruptedException) {
                    System.out.println("Reconnection attempt interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    
        return reconnected;
    }
    
    private void performReconnectedActions() {
        coordinator.getClientSockets().add(clientSocket);
        coordinator.getFileSocketMap().put(file, clientSocket);
        SocketHandler newSocketHandler = new SocketHandler(coordinator, file, taskId, phase);
        if (keyManager.getFinalAssignments().get(this) != null) {
            System.out.println("Reassigning keys to the new worker...");
            List<Integer> keys= keyManager.getFinalAssignments().get(this);
            keyManager.getFinalAssignments().remove(this);            
            keyManager.getFinalAssignments().put(newSocketHandler, keys);
        }
        System.out.println("Reconnected to a new worker. Resuming operations...");
        newSocketHandler.run();
    }
}
