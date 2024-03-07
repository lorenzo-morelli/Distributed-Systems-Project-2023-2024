package it.polimi.worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.common.messages.NormalOperations;

class WorkerHandler extends Thread {
    private Socket clientSocket;
    private int identifier;
    private String programId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private List<Operator> operators;
    //private CheckPointManager checkPointManager;
    private HadoopWorker hadoopWorker;
    public WorkerHandler(Socket clientSocket, HadoopWorker hadoopWorker) {
        this.clientSocket = clientSocket;
        //this.checkPointManager = new CheckPointManager();
        this.hadoopWorker = hadoopWorker;
        this.identifier = -1;
        this.programId = null;
        this.operators = new ArrayList<>();
    }

    @Override
    public void run() {
        Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getLocalPort() + "(" +clientSocket.getPort() + ")");

        logger.info(Thread.currentThread().getName() + ": WorkerHandler started.");

        System.out.println(Thread.currentThread().getName() + ": WorkerHandler started");
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {

            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            while (true) {

                // Read the object from the coordinator
                Object object = inputStream.readObject();
                System.out.println(Thread.currentThread().getName() + ": Received message from coordinator");
                logger.info(Thread.currentThread().getName()+ ": Received message from coordinator");
                if (object instanceof NormalOperations) {

                    NormalOperations task = (NormalOperations) object;
                    identifier = task.getIdentifier();
                    programId = task.getProgramId();  
                    Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getLocalPort() + "(" +clientSocket.getPort() + ")" + ":" + task.getProgramId());
                    System.out.println(Thread.currentThread().getName() + ": Received task from coordinator");
                    logger.info(Thread.currentThread().getName() + ": Received task from coordinator: " + identifier);
                    
                                      
                    try{
                        // Process the Task
                        processTask(task);
                    }
                    catch(IOException | IllegalArgumentException e ){
                        logger.error(Thread.currentThread().getName() + ": Error while processing the task: " + e.getMessage());
                        outputStream.writeObject(new ErrorMessage(e.getMessage()));
                        System.out.println(Thread.currentThread().getName() + ": Error while processing the task\n" + e.getMessage());
                        break;
                    }
                    outputStream.writeObject(true);
                    System.out.println(Thread.currentThread().getName() + ": Keys sent to the coordinator");
                    logger.info(Thread.currentThread().getName() + ": Keys sent to the coordinator");
                    if(!(task.getChangeKey() && task.getReduce())){
                        break;
                    }
                } else if (object instanceof ReduceOperation){
                    ReduceOperation reduceMessage = (ReduceOperation) object;
                    identifier = reduceMessage.getIdentifier();
                    programId = reduceMessage.getProgramId();

                    Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getLocalPort() + "(" +clientSocket.getPort() + ")" + ":" + reduceMessage.getProgramId());

                    System.out.println(Thread.currentThread().getName() + ": Received LastReduce message from coordinator");
                    logger.info(Thread.currentThread().getName() + ": Received LastReduce message from coordinator, responsible for the keys: " + reduceMessage.getKeys());
                    
                    try{
                        computeReduceMessage(reduceMessage);
                    }
                    catch(IOException | IllegalArgumentException e){
                        outputStream.writeObject(new ErrorMessage("Error in the reduce phase"));
                        logger.error(Thread.currentThread().getName() + ": Error in the reduce phase: " + e.getMessage());
                        break;
                    }
                    outputStream.writeObject(true);
                    break;
                }
                else {
                    // Handle other types or unexpected objects
                    System.out.println(Thread.currentThread().getName() + ": Received unexpected object type");
                    outputStream.writeObject(new ErrorMessage("Received unexpected object type"));
                    logger.error(Thread.currentThread().getName() + ": Received unexpected object type");
                    break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println(Thread.currentThread().getName()+": Coordinator connection lost");
            logger.error(Thread.currentThread().getName() + ": Coordinator connection lost: " + e.getMessage());
        } finally {
            System.out.println(Thread.currentThread().getName() + ": Closing connection");
            logger.info(Thread.currentThread().getName() + ": Closing connection");
            try {
                // Close the streams and socket when done
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                logger.error(Thread.currentThread().getName() + ": Error while closing the connection: " + e.getMessage());
                System.out.println(Thread.currentThread().getName() + ": "+ e.getMessage());
            }
        }
    }

    private List<Operator> handleOperators(List<MutablePair<String, String>> dataFunctions) {
        logger.info(Thread.currentThread().getName() + ": Handling operators");
        List<Operator> operators = new ArrayList<>();

        for (MutablePair<String, String> df : dataFunctions) {
            String op = df.getLeft();
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
            logger.info(Thread.currentThread().getName()+ ": Operator created: " + op + " with function: " + fun);
        }

        return operators;
    }
   

    private void processTask(NormalOperations task) throws IOException{
        operators = handleOperators(task.getOperators());
        System.out.println(Thread.currentThread().getName() + ": Processing task" + task.getPathFiles());
        for(int i = 0;i<task.getPathFiles().size();i++){
            hadoopWorker.readInputFile(i,task,this,operators);
        }
    }
    public void processPartitionTask(List<KeyValuePair> result,NormalOperations task, Integer numFile,Integer numPart) throws IOException{
        
        hadoopWorker.writeKeys(programId,identifier + "_" + numFile +"_" + numPart,result,task.getChangeKey(),task.getReduce());
    }

    private void computeReduceMessage(ReduceOperation reduceMessage) throws IOException{
        Operator reduce = handleOperators(List.of(reduceMessage.getReduce())).get(0);
        for(int idx = reduceMessage.getKeys().getLeft(); idx < reduceMessage.getKeys().getRight(); idx++ ){   
            KeyValuePair result = hadoopWorker.readAndComputeReduce(idx,reduceMessage,reduce);
            hadoopWorker.writeKeys(programId,String.valueOf(identifier),result);
        } 
    }
}