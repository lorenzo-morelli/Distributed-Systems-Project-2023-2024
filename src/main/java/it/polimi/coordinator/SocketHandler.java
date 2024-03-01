package it.polimi.coordinator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.common.Address;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.common.messages.NormalOperations;

public class SocketHandler implements Runnable {
    public Socket clientSocket;
    private int identifier;
    private ProgramExecutor programExecutor;
    private List<String> files;
    private KeyAssignmentManager keyManager;
    private CoordinatorPhase phase;
    private ObjectInputStream inputStream = null;
    private ObjectOutputStream outputStream = null;
    private boolean isProcessing;
    private String programId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");

    public SocketHandler(ProgramExecutor programExecutor, List<String> files, int identifier,CoordinatorPhase phase) {
        this.clientSocket = programExecutor.getFileSocketMap().get(files);
        this.keyManager = programExecutor.getKeyManager();
        this.files = files;
        this.identifier = identifier;
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

                        NormalOperations t = new NormalOperations(programId,programExecutor.getOperations(), files,programExecutor.getChangeKey(),programExecutor.getReduce(),identifier);
                        System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase1: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                        logger.info(Thread.currentThread().getName() + ": Sending task to worker phase1: "+ clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                        outputStream.writeObject(t);
                        
                        Object object = inputStream.readObject();
                        if (object == null){ 
                            logger.error(Thread.currentThread().getName() +": Received a null object!");
                            System.out.println(Thread.currentThread().getName() + ": Received a null object!"); 
                        }else if (object instanceof ErrorMessage) {
                            logger.error(Thread.currentThread().getName() +": Received an error message!" + ((ErrorMessage) object).getMessage());
                            System.out.println(Thread.currentThread().getName() + ": " + ((ErrorMessage) object).getMessage());
                            programExecutor.setErrorPresent(true);
                            isProcessing = false;
                        }else if (object instanceof Boolean) {
                            // Process or print the list
                            if (!(programExecutor.getChangeKey() && programExecutor.getReduce())) {
                                System.out.println(Thread.currentThread().getName() + ": Received the final result");
                                logger.info(Thread.currentThread().getName() + ": Received the final result");
                                end();
                                isProcessing = false;
                            } else {
                                System.out.println(Thread.currentThread().getName() + ": Received the keys to be managed");
                                logger.info(Thread.currentThread().getName() + ": Received the keys to be managed");
                                managePhase2();
                            }
                        } 
                        break;
            
                    case FINAL:
                        if(programExecutor.IsErrorPresent()){
                            logger.error(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
                            System.out.println(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
                            isProcessing = false;
                            break;
                        }
                        if(keyManager.canProceed()){
                            logger.info(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                            System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());
                            ReduceOperation lastReduce = new ReduceOperation(programId,programExecutor.getLastReduce(), keyManager.getFinalAssignments().get(this),identifier);
                            outputStream.writeObject(lastReduce);                
                            Object finalObject = inputStream.readObject();
                            if (finalObject == null ) {
                                logger.error(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                                System.out.println(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                            }else if (finalObject instanceof ErrorMessage){
                                logger.error(Thread.currentThread().getName() + ": Received an error message!" + ((ErrorMessage) finalObject).getMessage());
                                System.out.println(Thread.currentThread().getName()+ ": " + ((ErrorMessage) finalObject).getMessage());
                                programExecutor.setErrorPresent(true);
                                isProcessing = false;
                            }else if (finalObject instanceof Boolean) {
                                System.out.println(Thread.currentThread().getName() + ": Received the final result");
                                logger.info(Thread.currentThread().getName() + ": Received the final result");  
                                end();
                                isProcessing = false;
                            }            
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
    private void end(){
        try{
            programExecutor.manageEnd(identifier);   
        }catch(Exception e){
            System.out.println(Thread.currentThread().getName() + ": Error while managing the end of the program" + e.getMessage());
            logger.error(Thread.currentThread().getName() + ": Error while managing the end of the program" + e.getMessage());
            System.exit(0);
        }
    }

    
    public void managePhase2(){

        try{
            keyManager.insertAssignment(this,programExecutor.getNumPartitions());
            this.phase = CoordinatorPhase.FINAL;
        }catch(Exception e){
            System.out.println(Thread.currentThread().getName() + ": Error while splitting the keys" + e.getMessage());
            logger.error(Thread.currentThread().getName() + ": Error while splitting th keys" + e.getMessage());
            System.exit(0);
        }
    }

    private void handleSocketException() {
        logger.info(Thread.currentThread().getName() + ": Handling socket exception...");
        
        programExecutor.getClientSockets().remove(clientSocket);
        programExecutor.getFileSocketMap().put(files, null);
    
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
        programExecutor.getFileSocketMap().put(files, clientSocket);
        SocketHandler newSocketHandler = new SocketHandler(programExecutor, files, identifier, phase);
        if (keyManager.getFinalAssignments().get(this) != null) {
            System.out.println(Thread.currentThread().getName() + ": Reassigning keys to the new worker...");
            logger.info(Thread.currentThread().getName() + ": Reassigning keys to the new worker...");
            MutablePair<Integer,Integer> keys= keyManager.getFinalAssignments().get(this);
            keyManager.getFinalAssignments().remove(this);            
            keyManager.getFinalAssignments().put(newSocketHandler, keys);
        }
        System.out.println(Thread.currentThread().getName() + ": Reconnected to a new worker. Resuming operations...");
        logger.info(Thread.currentThread().getName() + ": Reconnected to a new worker. Resuming operations...");    
        newSocketHandler.run();
    }
}