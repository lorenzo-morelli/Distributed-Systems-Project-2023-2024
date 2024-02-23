package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.common.Address;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.LastReduce;
import it.polimi.common.messages.Task;

public class SocketHandler implements Runnable {
    public Socket clientSocket;
    private Integer taskId;
    private ProgramExecutor programExecutor;
    private String file;
    private KeyAssignmentManager keyManager;
    private CoordinatorPhase phase;
    private ObjectInputStream inputStream = null;
    private ObjectOutputStream outputStream = null;
    private boolean isProcessing;
    private String programId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");

    public SocketHandler(ProgramExecutor programExecutor, String file, Integer taskId,CoordinatorPhase phase) {
        this.clientSocket = programExecutor.getFileSocketMap().get(file);
        this.keyManager = programExecutor.getKeyManager();
        this.file = file;
        this.taskId = taskId;
        this.programExecutor = programExecutor;
        this.phase = phase;
        this.isProcessing = true;
        this.programId = programExecutor.getProgramId();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort() + "(" +clientSocket.getLocalPort() + "):" + programId);

        logger.info(Thread.currentThread().getName() + ": Starting worker connection");
        try {
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            
            while (isProcessing) {
                switch (phase) {
                    case INIT:
                        Task t = new Task(programId,programExecutor.getOperations(), file,programExecutor.checkChangeKeyReduce(),taskId);
                        System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase1: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                        logger.info(Thread.currentThread().getName() + ": Sending task to worker phase1: "+ clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                        outputStream.writeObject(t);
                        Object object = inputStream.readObject();
                        if (object == null){ 
                            logger.error(Thread.currentThread().getName() +": Received a null object!");
                            System.out.println(Thread.currentThread().getName() + ": Received a null object!");
                            System.exit(0);
                        }else if (object instanceof ErrorMessage) {
                            logger.error(Thread.currentThread().getName() +": Received an error message!" + ((ErrorMessage) object).getMessage());
                            System.out.println(Thread.currentThread().getName() + ": " + ((ErrorMessage) object).getMessage());
                            System.exit(0);
                        }else if (object instanceof List<?>) {
                            List<?> list = (List<?>) object;
                            // Process or print the list
                            if (!programExecutor.checkChangeKeyReduce()) {
                                logger.info(Thread.currentThread().getName() + ": Received the final result");
                                end(object);
                                isProcessing = false;
                            } else {
                                logger.info(Thread.currentThread().getName() + ": Received the keys to be managed");
                                managePhase2(list);
                            }
                        } 
                        break;
            
                    case FINAL:
                        if(keyManager.canProceed()){

                            logger.info(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                            System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                            LastReduce lastReduce = new LastReduce(programId,programExecutor.getLastReduce(), keyManager.getFinalAssignments().get(this));
                            outputStream.writeObject(lastReduce);
        
                        
                            Object finalObject = inputStream.readObject();
                            if (finalObject == null ) {
                                logger.error(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                                System.out.println(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                                System.exit(0);
                            }else if (finalObject instanceof ErrorMessage){
                                logger.error(Thread.currentThread().getName() + ": Received an error message!" + ((ErrorMessage) finalObject).getMessage());
                                System.out.println(Thread.currentThread().getName()+ ": " + ((ErrorMessage) finalObject).getMessage());
                                System.exit(0);
                            }else if (finalObject instanceof List<?>) {
                                logger.info(Thread.currentThread().getName() + ": Received the final result:" + finalObject);  
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
            System.out.println(Thread.currentThread().getName() + ": Worker connection lost");
            logger.error(Thread.currentThread().getName() + ": Worker connection lost");
            handleSocketException();
        }
    }
    private void end(Object object){
        try{
            logger.info(Thread.currentThread().getName() + ": Writing the final result...");
            programExecutor.writeResult(convertObjectToListKeyValuePairs(object));
            logger.info(Thread.currentThread().getName() + ": Final result written");
        }catch(Exception e){
            System.out.println(Thread.currentThread().getName() +": Error while writing the final result" + e.getMessage());
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
    public void managePhase2(List<?> list){
        List<Integer> integerList = new ArrayList<>();
        for (Object element : list) {
            if (element instanceof Integer) {
                integerList.add((Integer) element);
            }
        }
        logger.info(Thread.currentThread().getName() + ": Inserting keys...");
        keyManager.insertAssignment(this, integerList,programExecutor.getNumPartitions());
        this.phase = CoordinatorPhase.FINAL;
    }
    private void handleSocketException() {
        logger.info(Thread.currentThread().getName() + ": Handling socket exception...");
        
        programExecutor.getClientSockets().remove(clientSocket);
        programExecutor.getFileSocketMap().put(file, null);
    
        if (isProcessing) {
            boolean reconnected = attemptReconnection(clientSocket, 3, 5000);
    
            if (!reconnected) {
                logger.error(Thread.currentThread().getName() + ": Not possible to reconnect to the failed worker. Assigning to another worker...");
                System.out.println(Thread.currentThread().getName()+ ": Not possible to reconnect to the failed worker. Assigning to another worker...");
                reconnected = attemptReconnectionFromPool(3, 5000);
    
                if (!reconnected) {
                    logger.error(Thread.currentThread().getName() + ": Not possible to connect to any worker.");
                    System.out.println(Thread.currentThread().getName() + ": Not possible to connect to any worker.");
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
                logger.info(Thread.currentThread().getName() + ": Reconnected to the failed worker. Resuming operations...");
            } catch (IOException e) {
                attempts++;
                System.out.println(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");
                logger.error(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");
    
                try {
                    Thread.sleep(reconnectDelayMillis);
                } catch (InterruptedException interruptedException) {
                    System.out.println(Thread.currentThread().getName() + ": Reconnection attempt interrupted.");
                    logger.error(Thread.currentThread().getName() + ": Reconnection attempt interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    
        return reconnected;
    }
    
    private boolean attemptReconnectionFromPool(int maxAttempts, long reconnectDelayMillis) {
        boolean reconnected = false;
        int attempts = 0;
        
        logger.info(Thread.currentThread().getName() + ": Attempting to reconnect to another worker...");

        while (!reconnected && attempts < maxAttempts) {
            try {
                List<Address> addresses = new ArrayList<>(programExecutor.getAddresses());
                addresses.remove(new Address(clientSocket.getInetAddress().getHostName(), clientSocket.getPort()));
                logger.info(Thread.currentThread().getName() + ": Searching for a new worker, among" + addresses.size() + " available workers..." + addresses);
                if(addresses.size() == 0){
                    return false;
                }
                clientSocket = programExecutor.getNewActiveSocket(addresses, clientSocket.getInetAddress().getHostName());
                reconnected = true;
                logger.info(Thread.currentThread().getName() + ": Reconnected to a new worker" + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort() + ". Resuming operations...");
            } catch (Exception e) {
                


                attempts++;
                System.out.println(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");
                logger.error(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");
                try {
                    Thread.sleep(reconnectDelayMillis);
                } catch (InterruptedException interruptedException) {
                    System.out.println(Thread.currentThread().getName()+ ": Reconnection attempt interrupted.");
                    logger.error(Thread.currentThread().getName() + ": Reconnection attempt interrupted.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    
        return reconnected;
    }
    
    private void performReconnectedActions() {
        programExecutor.getClientSockets().add(clientSocket);
        programExecutor.getFileSocketMap().put(file, clientSocket);
        SocketHandler newSocketHandler = new SocketHandler(programExecutor, file, taskId, phase);
        if (keyManager.getFinalAssignments().get(this) != null) {
            System.out.println(Thread.currentThread().getName() + ": Reassigning keys to the new worker...");
            logger.info(Thread.currentThread().getName() + ": Reassigning keys to the new worker...");
            List<Integer> keys= keyManager.getFinalAssignments().get(this);
            keyManager.getFinalAssignments().remove(this);            
            keyManager.getFinalAssignments().put(newSocketHandler, keys);
        }
        System.out.println(Thread.currentThread().getName() + ": Reconnected to a new worker. Resuming operations...");
        logger.info(Thread.currentThread().getName() + ": Reconnected to a new worker. Resuming operations...");    
        newSocketHandler.run();
    }
}