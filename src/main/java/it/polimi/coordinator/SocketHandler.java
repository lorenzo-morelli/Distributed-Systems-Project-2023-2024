package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;


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
    private volatile boolean canProceedStep2;
    private ObjectInputStream inputStream = null;
    private ObjectOutputStream outputStream = null;
    private boolean isProcessing;
    public SocketHandler(Coordinator coordinator, String file, Integer taskId,CoordinatorPhase phase,boolean canProceedStep2) {
        this.clientSocket = coordinator.getFileSocketMap().get(file);
        this.keyManager = coordinator.getKeyManager();
        this.file = file;
        this.taskId = taskId;
        this.coordinator = coordinator;
        this.canProceedStep2 = canProceedStep2;
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
                            isProcessing = false;
                        }else if (object instanceof List<?>) {
                            List<?> list = (List<?>) object;
                            // Process or print the list
                            if (!coordinator.checkChangeKeyReduce()) {
                                System.out.println(list);
                                isProcessing = false;
                            } else {
                                managePhase2(list);
                            }
                        } else if (object instanceof ErrorMessage) {
                            System.out.println(((ErrorMessage) object).getMessage());
                        }
                        break;
            
                    case FINAL:
                        if(canProceedStep2){
                            
                            System.out.println("Sending task to worker phase2");
                            LastReduce lastReduce = new LastReduce(coordinator.getLastReduce(), keyManager.getFinalAssignments().get(this));
                            outputStream.writeObject(lastReduce);
        
                        
                            Object finalObject = inputStream.readObject();
                            if (finalObject == null ) {
                                System.out.println("Something went wrong with the reduce phase!");
                            }else if (finalObject instanceof ErrorMessage){
                                System.out.println(((ErrorMessage) finalObject).getMessage());
                            }else if (finalObject instanceof List<?>) {
                                System.out.println(finalObject);
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
    public void sendNewAssignment() {
        this.canProceedStep2 = true;
    }

    public void managePhase2(List<?> list){
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
            boolean reconnected = attemptReconnection(clientSocket, 3, 5000);
    
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
    
    private boolean attemptReconnection(Socket socket, int maxAttempts, long reconnectDelayMillis) {
        boolean reconnected = false;
        int attempts = 0;
    
        while (!reconnected && attempts < maxAttempts) {
            try {
                clientSocket = new Socket(socket.getInetAddress().getHostName(), socket.getPort());
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
                clientSocket = coordinator.getNewActiveSocket(new ArrayList<>(coordinator.getAddresses()));
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
        SocketHandler newSocketHandler = new SocketHandler(coordinator, file, taskId, phase, canProceedStep2);
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
