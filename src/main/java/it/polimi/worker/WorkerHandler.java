package it.polimi.worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.EndComputation;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.common.messages.StopComputation;
import it.polimi.worker.models.CheckpointInfo;
import it.polimi.worker.models.Operator;
import it.polimi.common.messages.NormalOperations;

/**
 * The WorkerHandler class is used to handle the communication between the Coordinator and the Worker.
 * It is used to process the tasks and the reduce messages received from the Coordinator.
 * It is also used to handle the operators and the checkpoints.
 */
public class WorkerHandler extends Thread {
    private final Socket clientSocket;
    private int identifier;
    private String programId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private List<Operator> operators;
    private final CheckPointManager checkPointManager;
    private final HadoopWorker hadoopWorker;
    private boolean safeDelete = false;

    /**
     * Constructor for the WorkerHandler class.
     * @param clientSocket The socket used to communicate with the Coordinator.
     * @param hadoopWorker The HadoopWorker used to read and write files.
     */
    public WorkerHandler(Socket clientSocket, HadoopWorker hadoopWorker) {
        this.clientSocket = clientSocket;
        this.checkPointManager = new CheckPointManager();
        this.hadoopWorker = hadoopWorker;
        this.identifier = -1;
        this.programId = null;
        this.operators = new ArrayList<>();
    }

    /**
     * The run method is used to handle the communication between the Coordinator and the Worker.
     * This method creates input and output streams for communication.
     * It is used to process the tasks and the reduce messages received from the Coordinator.
     */
    @Override
    public void run() {
        Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getLocalPort() + "(" + clientSocket.getPort() + ")");

        logger.info(Thread.currentThread().getName() + ": WorkerHandler started.");

        System.out.println(Thread.currentThread().getName() + ": WorkerHandler started");
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {

            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            while (true) {

                Object object = inputStream.readObject();
                System.out.println(Thread.currentThread().getName() + ": Received message from coordinator");
                logger.info(Thread.currentThread().getName() + ": Received message from coordinator");
                if (object instanceof NormalOperations task) {

                    identifier = task.getIdentifier();
                    programId = task.getProgramId();
                    Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getLocalPort() + "(" + clientSocket.getPort() + ")" + ":" + task.getProgramId());
                    System.out.println(Thread.currentThread().getName() + ": Received task from coordinator");
                    logger.info(Thread.currentThread().getName() + ": Received task from coordinator: " + identifier);


                    try {
                        if (processTask(task)) {
                            outputStream.writeObject(new EndComputation());
                            System.out.println(Thread.currentThread().getName() + ": EndComputation message sent to the coordinator");
                            logger.info(Thread.currentThread().getName() + ": EndComputation message sent to the coordinator");
                            if (!(task.getChangeKey() && task.getReduce())) {
                                safeDelete = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error(Thread.currentThread().getName() + ": Error while processing the task: " + e.getMessage());
                        outputStream.writeObject(new ErrorMessage(e.getMessage()));
                        System.out.println(Thread.currentThread().getName() + ": Error while processing the task\n" + e.getMessage());
                        break;
                    }


                } else if (object instanceof ReduceOperation reduceMessage) {
                    identifier = reduceMessage.getIdentifier();
                    programId = reduceMessage.getProgramId();

                    Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() + ":" + clientSocket.getLocalPort() + "(" + clientSocket.getPort() + ")" + ":" + reduceMessage.getProgramId());

                    System.out.println(Thread.currentThread().getName() + ": Received LastReduce message from coordinator");
                    logger.info(Thread.currentThread().getName() + ": Received LastReduce message from coordinator, responsible for the keys: " + reduceMessage.getKeys().getLeft() + "-" + reduceMessage.getKeys().getRight());

                    try {
                        if (computeReduceMessage(reduceMessage)) {
                            outputStream.writeObject(new EndComputation());
                            safeDelete = true;
                        }
                    } catch (IllegalArgumentException e) {
                        outputStream.writeObject(new ErrorMessage("Error in the reduce phase"));
                        logger.error(Thread.currentThread().getName() + ": Error in the reduce phase: " + e.getMessage());
                    }
                    break;

                }else if(object instanceof StopComputation){
                    System.out.println(Thread.currentThread().getName() + ": Received StopComputation message from coordinator");
                    logger.info(Thread.currentThread().getName() + ": Received StopComputation message from coordinator");
                    safeDelete = true;
                    break;
                }  
                else {
                    System.out.println(Thread.currentThread().getName() + ": Received unexpected object type");
                    outputStream.writeObject(new ErrorMessage("Received unexpected object type"));
                    logger.error(Thread.currentThread().getName() + ": Received unexpected object type");
                    break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println(Thread.currentThread().getName() + ": Coordinator connection lost");
            logger.error(Thread.currentThread().getName() + ": Coordinator connection lost: " + e.getMessage());
        } finally {
            System.out.println(Thread.currentThread().getName() + ": Closing connection");
            logger.info(Thread.currentThread().getName() + ": Closing connection");


            try {
                Thread.sleep(2000);
                if (safeDelete) {
                    hadoopWorker.closeFileSystem();
                    checkPointManager.deleteCheckpoints(programId);
                }

            } catch (InterruptedException e) {
                logger.error(Thread.currentThread().getName() + ": Error while sleeping: " + e.getMessage());
                System.out.println(Thread.currentThread().getName() + ": Error while sleeping: " + e.getMessage());
            }

            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                logger.error(Thread.currentThread().getName() + ": Error while closing the connection: " + e.getMessage());
                System.out.println(Thread.currentThread().getName() + ": " + e.getMessage());
            }
        }
    }

    /**
     * The handleOperators method is used to create the operators from a list of mutable pairs.
     * @param dataFunctions it is the data functions to be used to create the operators.
     * @return The list of operators created.
     */
    private List<Operator> handleOperators(List<MutablePair<String, String>> dataFunctions) {
        logger.info(Thread.currentThread().getName() + ": Handling operators");
        List<Operator> operators = new ArrayList<>();

        for (MutablePair<String, String> df : dataFunctions) {
            String op = df.getLeft();
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
            logger.info(Thread.currentThread().getName() + ": Operator created: " + op + " with function: " + fun);
        }

        return operators;
    }

    /**
     * The processTask method is used to process the task received from the Coordinator.
     * It reads the input files and processes them using the operators.
     * It also creates the checkpoints for the task.
     * @param task it is the task to be processed.
     * @return true if the task was processed successfully, false otherwise.
     */
    private boolean processTask(NormalOperations task) {

        operators = handleOperators(task.getOperators());

        try {
            for (int i = 0; i < task.getPathFiles().size(); i++) {
                CheckpointInfo checkPointObj;
               
                checkPointObj = checkPointManager.getCheckPoint(task.getProgramId(), task.getPathFiles().get(i),task.getReduce() && !task.getChangeKey());
               

                if (checkPointObj.end()) {
                    logger.info(Thread.currentThread().getName() + ": File already processed");
                } else {
                    if (checkPointObj.count() != 0) {
                        logger.info(Thread.currentThread().getName() + ": File partially processed, resuming from partition: " + checkPointObj.count());
                    } else {
                        logger.info(Thread.currentThread().getName() + ": File not processed yet");
                    }

                    hadoopWorker.readInputFile(i, task, this, operators, checkPointObj.count(), checkPointObj.remainingString(), checkPointObj.keyValuePair());
                }

            }
            return true;
        
        } catch (IOException e) {
            if(e instanceof ConnectException){
                logger.error(Thread.currentThread().getName() + ": Error while connecting to the HDFS: " + e.getMessage());
                System.out.println(Thread.currentThread().getName() + ": Error while connecting to the HDFS\n" + e.getMessage());
                System.exit(0);
            }
            logger.error(Thread.currentThread().getName() + ": Error while processing the task: " + e.getMessage());
            System.out.println(Thread.currentThread().getName() + ": Error while processing the task\n" + e.getMessage());
        } 
        return false;
    }
    /**
     * The processPartitionTask method is used to write the keys and create the checkpoints for a partition.
     * This method is invoked by the HadoopWorker after processing a partition. 
     * @param result it is the result of the partition.
     * @param task it is the task to be processed.
     * @param numFile it is the number of the file to be processed.
     * @param numPart it is the number of the partition to be processed.
     * @param end it is a boolean value indicating if the partition is the last one.
     * @param remainingString it is the remaining string to be processed.
     * @param writeKeys it is a boolean value indicating if the keys should be written.
     * @throws IOException if an error occurs while writing the keys or creating the checkpoints.
     */
    public void processPartitionTask(List<KeyValuePair> result, NormalOperations task, Integer numFile, Integer numPart, Boolean end, String remainingString, boolean writeKeys) throws IOException {
        if ((task.getReduce() && !task.getChangeKey())) {
            if (writeKeys) {
                hadoopWorker.writeKeys(programId, identifier + "_" + numFile + "_" + numPart, result, task.getChangeKey(), task.getReduce());
            }
            checkPointManager.createCheckpoint(programId, task.getPathFiles().get(numFile), new CheckpointInfo(numPart, end, remainingString, result.getFirst()), true);
        } else {
            hadoopWorker.writeKeys(programId, identifier + "_" + numFile + "_" + numPart, result, task.getChangeKey(), task.getReduce());
            checkPointManager.createCheckpoint(programId, task.getPathFiles().get(numFile), new CheckpointInfo(numPart, end, remainingString, null), false);
        }
    }
    /**
     * The computeReduceMessage method is used to process the reduce message received from the Coordinator.
     * It reads the input files and processes them using the reduce operator.
     * It also retrieves the checkpoints for the reduce message.
     * @param reduceMessage it is the reduce message to be processed.
     * @return true if the reduce message was processed successfully, false otherwise.
     */
    private boolean computeReduceMessage(ReduceOperation reduceMessage) {
        Operator reduce = handleOperators(List.of(reduceMessage.getReduce())).getFirst();

        try {
            CheckpointInfo checkPointObj = checkPointManager.getCheckPoint(reduceMessage.getProgramId(), "reduce" + identifier + ".csv",true);

            int start = checkPointObj.count()>0  ? checkPointObj.count() : reduceMessage.getKeys().getLeft();
            
            if(checkPointObj.count() > 0){  
                logger.info(Thread.currentThread().getName() + ": Files partially processed, resuming from idx: " + start); 
            }
            else {
                logger.info(Thread.currentThread().getName() + ": Files not processed yet");
            }
            
            for (int idx = start; idx < reduceMessage.getKeys().getRight(); idx++) {
                
                
                if (checkPointObj.end()) {
                    logger.info(Thread.currentThread().getName() + ": File with idx: " + idx + " already processed");
                    checkPointObj = new CheckpointInfo(0, false, "", null);
                } else {
                    KeyValuePair result = hadoopWorker.readAndComputeReduce(idx, reduceMessage, reduce,this, checkPointObj);
                    if(result!= null){
                        hadoopWorker.writeKeys(programId, String.valueOf(identifier), result);
                    }
                 }
                
            }
            return true;
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while processing the reduce phase: " + e.getMessage());
            System.out.println(Thread.currentThread().getName() + ": Error while processing the reduce phase\n" + e.getMessage());
        }
        return false;
    }
    /**
     * The createCheckpoint method is used to create the checkpoints for a reduce message.
     * This method is invoked by the HadoopWorker after processing a partition in the second phase.
     * @param result it is the result of the partition.
     * @param idx it is the index of the partition processed.
     * @param numFile it is the number of the last file processed.
     * @param end it is a boolean value indicating if the partition is the last one.
     */
    public void createCheckpoint(KeyValuePair result, Integer idx,Integer numFile, Boolean end) {
        checkPointManager.createCheckpoint(programId, "reduce" + identifier + ".csv", new CheckpointInfo(idx, end, String.valueOf(numFile), result), true);
    }

}