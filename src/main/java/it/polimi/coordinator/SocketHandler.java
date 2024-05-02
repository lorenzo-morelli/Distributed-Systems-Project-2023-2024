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
import it.polimi.common.messages.EndComputation;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.common.messages.StopComputation;
import it.polimi.common.messages.NormalOperations;

/**
 * The SocketHandler class is used to manage the connection with the worker node.
 * It contains the client socket, the identifier of the worker, the program executor, the list of files, the key assignment manager and the phase of the coordinator.
 */
public class SocketHandler implements Runnable {
    public Socket clientSocket;
    private final int identifier;
    private final ProgramExecutor programExecutor;
    private final List<String> files;
    private final KeyAssignmentManager keyManager;
    private ProgramPhase phase;
    private boolean isProcessing;
    private final String programId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    public static final int WAIT_TIME = 5000;
    public static final int MAX_RETRIES = 3;

    /**
     * The constructor creates a new SocketHandler.
     *
     * @param programExecutor represents the program executor.
     * @param files           represents the list of files to process.
     * @param identifier      represents the identifier of the worker for a specific program.
     * @param phase           represents the phase of the program.
     */
    public SocketHandler(ProgramExecutor programExecutor, List<String> files, int identifier, ProgramPhase phase) {
        this.clientSocket = programExecutor.getFileSocketMap().get(files);
        this.keyManager = programExecutor.getKeyManager();
        this.files = files;
        this.identifier = identifier;
        this.programExecutor = programExecutor;
        this.phase = phase;
        this.isProcessing = true;
        this.programId = programExecutor.getProgramId();
    }

    /**
     * The run method is used to manage the connection with the worker node.
     * It sends the message to the worker and receives the result.
     * It manages the reconnection to the worker in case of failure.
     */
    @Override
    public void run() {
        Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort() + "(" + clientSocket.getLocalPort() + "):" + programId);

        logger.info(Thread.currentThread().getName() + ": Starting worker connection");
        try {
            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());

            while (isProcessing) {
                switch (phase) {
                    case INIT:

                        NormalOperations task = new NormalOperations(programId, programExecutor.getOperations(), files, programExecutor.getChangeKey(), programExecutor.getReduce(), identifier);
                        System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase1: " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort());
                        logger.info(Thread.currentThread().getName() + ": Sending task to worker phase1: " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort());
                        outputStream.writeObject(task);

                        Object object = inputStream.readObject();
                        switch (object) {
                            case null -> {
                                logger.error(Thread.currentThread().getName() + ": Received a null object!");
                                System.out.println(Thread.currentThread().getName() + ": Received a null object!");
                            }
                            case ErrorMessage errorMessage -> {
                                logger.error(Thread.currentThread().getName() + ": Received an error message!" + errorMessage.message());
                                System.out.println(Thread.currentThread().getName() + ": " + errorMessage.message());
                                programExecutor.setErrorPresent(true);
                                isProcessing = false;
                            }
                            case EndComputation ignored -> {
                                if (!(programExecutor.getChangeKey() && programExecutor.getReduce())) {
                                    System.out.println(Thread.currentThread().getName() + ": Received the final result");
                                    logger.info(Thread.currentThread().getName() + ": Received the final result");
                                    end();
                                    isProcessing = false;
                                } else {
                                    System.out.println(Thread.currentThread().getName() + ": Received the message end computation for phase1");
                                    logger.info(Thread.currentThread().getName() + ": Received the message end computation for phase1");
                                    managePhase2();
                                }
                            }
                            default -> {
                            }
                        }
                        break;

                    case FINAL:
                        if (programExecutor.IsErrorPresent()) {
                            logger.error(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
                            System.out.println(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
                            isProcessing = false;
                            break;
                        }
                        if (keyManager.canProceed()) {
                            logger.info(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort());
                            System.out.println(Thread.currentThread().getName() + ": Sending task to worker phase2: " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort());
                            ReduceOperation lastReduce = new ReduceOperation(programId, programExecutor.getLastReduce(), keyManager.getAssignments().get(this), identifier);
                            outputStream.writeObject(lastReduce);
                            Object finalObject = inputStream.readObject();
                            switch (finalObject) {
                                case null -> {
                                    logger.error(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                                    System.out.println(Thread.currentThread().getName() + ": Something went wrong with the reduce phase!");
                                }
                                case ErrorMessage errorMessage -> {
                                    logger.error(Thread.currentThread().getName() + ": Received an error message!" + errorMessage.message());
                                    System.out.println(Thread.currentThread().getName() + ": " + errorMessage.message());
                                    programExecutor.setErrorPresent(true);
                                    isProcessing = false;
                                }
                                case EndComputation ignored -> {
                                    System.out.println(Thread.currentThread().getName() + ": Received the final result");
                                    logger.info(Thread.currentThread().getName() + ": Received the final result");
                                    end();
                                    isProcessing = false;
                                }
                                default -> {
                                }
                            }
                        }
                        if (keyManager.exit()) {
                            outputStream.writeObject(new StopComputation());
                            logger.info("No keys to process, exiting...");
                            isProcessing = false;
                            break;
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

    /**
     * The end method is used to manage the end of the program.
     * It calls the manageEnd method of the program executor.
     */
    private void end() {
        try {
            programExecutor.manageEnd();
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + ": Error while managing the end of the program" + e.getMessage());
            logger.error(Thread.currentThread().getName() + ": Error while managing the end of the program" + e.getMessage());
            System.exit(0);
        }
    }

    /**
     * The managePhase2 method is used to manage the second phase of the program.
     */
    public void managePhase2() {

        try {
            keyManager.insertAssignment(this, Math.min(programExecutor.getNumPartitions(), programExecutor.getFilesSize()));
            this.phase = ProgramPhase.FINAL;
        } catch (IllegalArgumentException e) {
            logger.info(Thread.currentThread().getName() + ": " + e.getMessage());
            this.phase = ProgramPhase.FINAL;
        } catch (IOException e) {
            System.out.println(Thread.currentThread().getName() + ": Error while splitting the keys" + e.getMessage());
            logger.error(Thread.currentThread().getName() + ": Error while splitting th keys" + e.getMessage());
            System.exit(0);
        }
    }

    /**
     * The handleSocketException method is used to manage the reconnection to the worker in case of failure.
     * It tries to reconnect to the failed worker and, if not possible, it assigns the task to another worker.
     * If the reconnection is successful, the task is reassigned to the new worker.
     * If it is not possible to connect to any worker, the program is stopped.
     */
    private void handleSocketException() {
        logger.info(Thread.currentThread().getName() + ": Handling socket exception...");

        programExecutor.getClientSockets().remove(clientSocket);
        programExecutor.getFileSocketMap().put(files, null);

        if (isProcessing) {
            boolean reconnected = attemptReconnection(clientSocket);

            if (!reconnected) {
                logger.error(Thread.currentThread().getName() + ": Not possible to reconnect to the failed worker. Assigning to another worker...");
                System.out.println(Thread.currentThread().getName() + ": Not possible to reconnect to the failed worker. Assigning to another worker...");
                reconnected = attemptReconnectionFromPool();

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

    /**
     * The attemptReconnection method is used to try to reconnect to the failed worker.
     *
     * @param socket represents the socket of the failed worker.
     * @return true if the reconnection is successful, false otherwise.
     */
    private boolean attemptReconnection(Socket socket) {
        boolean reconnected = false;
        int attempts = 0;

        while (!reconnected && attempts < MAX_RETRIES) {
            try {
                clientSocket = new Socket(socket.getInetAddress().getHostName(), socket.getPort());
                reconnected = true;
                System.out.println(Thread.currentThread().getName() + ": Reconnected to the failed worker. Resuming operations...");
                logger.info(Thread.currentThread().getName() + ": Reconnected to the failed worker. Resuming operations...");
            } catch (IOException e) {
                attempts = getAttempts(attempts);
            }
        }

        return reconnected;
    }

    /**
     * The getAttempts method is used to manage the reconnection attempts.
     *
     * @param attempts represents the number of attempts.
     * @return the number of attempts.
     */
    private int getAttempts(int attempts) {
        attempts++;
        System.out.println(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");
        logger.error(Thread.currentThread().getName() + ": Reconnection attempt " + attempts + " failed. Retrying...");

        try {
            Thread.sleep(WAIT_TIME);
        } catch (InterruptedException interruptedException) {
            System.out.println(Thread.currentThread().getName() + ": Reconnection attempt interrupted.");
            logger.error(Thread.currentThread().getName() + ": Reconnection attempt interrupted.");
            Thread.currentThread().interrupt();
        }
        return attempts;
    }

    /**
     * The attemptReconnectionFromPool method is used to try to reconnect to another worker.
     * It searches for a new worker among the available workers.
     *
     * @return true if the reconnection is successful, false otherwise.
     */
    private boolean attemptReconnectionFromPool() {
        boolean reconnected = false;
        int attempts = 0;

        logger.info(Thread.currentThread().getName() + ": Attempting to reconnect to another worker...");

        while (!reconnected && attempts < MAX_RETRIES) {
            try {
                List<Address> addresses = new ArrayList<>(programExecutor.getAddresses());
                addresses.remove(new Address(clientSocket.getInetAddress().getHostName(), clientSocket.getPort()));
                logger.info(Thread.currentThread().getName() + ": Searching for a new worker, among " + addresses.size() + " available workers..." + addresses);
                if (addresses.isEmpty()) {
                    return false;
                }
                clientSocket = Coordinator.getNewActiveSocket(addresses, clientSocket.getInetAddress().getHostName());
                reconnected = true;
                System.out.println(Thread.currentThread().getName() + ": Reconnected to a new worker " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort() + ". Resuming operations...");
                logger.info(Thread.currentThread().getName() + ": Reconnected to a new worker " + clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getPort() + ". Resuming operations...");
            } catch (Exception e) {


                attempts = getAttempts(attempts);
            }
        }

        return reconnected;
    }

    /**
     * The performReconnectedActions method is used to perform the actions after a successful reconnection.
     * It adds the new worker to the list of client sockets and updates the file socket map,
     * it creates a new SocketHandler for the new worker and reassigns the keys to the new worker, and
     * it resumes the operations with the new worker.
     */
    private void performReconnectedActions() {
        programExecutor.getClientSockets().add(clientSocket);
        programExecutor.getFileSocketMap().put(files, clientSocket);
        SocketHandler newSocketHandler = new SocketHandler(programExecutor, files, identifier, phase);
        if (keyManager.getAssignments().get(this) != null) {
            System.out.println(Thread.currentThread().getName() + ": Reassigning keys to the worker...");
            logger.info(Thread.currentThread().getName() + ": Reassigning keys to the worker...");
            MutablePair<Integer, Integer> keys = keyManager.getAssignments().get(this);
            keyManager.getAssignments().remove(this);
            keyManager.getAssignments().put(newSocketHandler, keys);
        }
        newSocketHandler.run();
    }
}