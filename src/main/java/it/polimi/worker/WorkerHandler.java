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
import it.polimi.common.messages.LastReduce;
import it.polimi.common.messages.Task;
import it.polimi.worker.operators.ReduceOperator;

class WorkerHandler extends Thread {
    private Socket clientSocket;
    private Integer taskId;
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private CheckPointManager checkPointManager;
    private HadoopWorker hadoopWorker;
    public WorkerHandler(Socket clientSocket, HadoopWorker hadoopWorker) {
        this.clientSocket = clientSocket;
        this.checkPointManager = new CheckPointManager();
        this.hadoopWorker = hadoopWorker;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort());

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
                logger.info(Thread.currentThread().getName()+ ": Received object from coordinator: " + object);
                if (object instanceof Task) {
                    Task task = (Task) object;
                    taskId = task.getTaskId(); 
                    List<KeyValuePair> result  = new ArrayList<>();
                    
                    try{
                        // Process the Task
                        result = processTask(task);
                    }
                    catch(IOException | IllegalArgumentException e ){
                        logger.error(Thread.currentThread().getName() + ": Error while processing the task: " + e.getMessage());
                        outputStream.writeObject(new ErrorMessage(e.getMessage()));
                        System.out.println(Thread.currentThread().getName() + ": Error while processing the task\n" + e.getMessage());
                        break;
                    }

                    if (result != null) {
                        if(!task.isPresentStep2()){
                            logger.info(Thread.currentThread().getName()+ ": Step 1 is done, sending result back to the coordinator: " + result);
                            // Send the result back to the coordinator
                            outputStream.writeObject(result);
                            break;
                        }else{
                            try{
                                logger.info(Thread.currentThread().getName() + ": Writing the keys in HDFS");
                                hadoopWorker.writeKeys(
                                    task.getProgramId(),
                                    taskId.toString(),
                                    result
                                    );
                                logger.info(Thread.currentThread().getName() + ": Keys written in HDFS");
                            }catch(IOException e){
                                logger.error(Thread.currentThread().getName()+ ": Error while writing the keys in HDFS: " + e.getMessage());
                                outputStream.writeObject(new ErrorMessage("Error while writing the keys in HDFS"));
                                break;
                            }
                            List<Integer> keys = extractKeys(result);
                            outputStream.writeObject(keys);
                            logger.info(Thread.currentThread().getName() + ": Keys sent to the coordinator :" + keys);

                        }
                    }else{
                        outputStream.writeObject(new ErrorMessage("Result is null!"));
                        logger.error(Thread.currentThread().getName() + ": Result is null!");
                        break;
                    }
                } else if (object instanceof LastReduce){
                    LastReduce reduceMessage = (LastReduce) object;

                    logger.info(Thread.currentThread().getName() + ": Received LastReduce message from coordinator, responsible for the keys: " + reduceMessage.getKeys());
                    
                    List<KeyValuePair> result = new ArrayList<>(); 
                    try{

                        result = computeReduceMessage(reduceMessage);
                    }
                    catch(IOException e){
                        outputStream.writeObject(new ErrorMessage("Error in the reduce phase"));
                        logger.error(Thread.currentThread().getName() + ": Error in the reduce phase: " + e.getMessage());
                        break;
                    }
                    outputStream.writeObject(result);
                    logger.info(Thread.currentThread().getName()+": " + result + " sent to the coordinator");
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
                hadoopWorker.closeFileSystem();
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
    private List<KeyValuePair> computeReduceMessage(LastReduce reduceMessage) throws IOException{
        Thread.currentThread().setName(Thread.currentThread().getName() + ":" + reduceMessage.getProgramId());

        logger.info(Thread.currentThread().getName()+ ": Computing reduce");
        List<KeyValuePair> result = new ArrayList<>();
        List<KeyValuePair> temp = new ArrayList<>();

        
        Integer sizeCheckPoint = 0;

        if(reduceMessage.getKeys().size() > 10){
            sizeCheckPoint = reduceMessage.getKeys().size()/10;
        }else{
            sizeCheckPoint = reduceMessage.getKeys().size();
        }
        logger.info(Thread.currentThread().getName()+ ": Checkpointing every " + sizeCheckPoint + " keys");
        Integer i = 0;
        Integer j = 0;
        for(Integer key: reduceMessage.getKeys()){

            logger.info(Thread.currentThread().getName()+ ": Check if a checkpoint exists for key" + key);
            MutablePair<Boolean, List<KeyValuePair>> checkPoint = checkPointManager.checkCheckPoint(key,reduceMessage.getProgramId(),true);
            logger.info(Thread.currentThread().getName()+ ": CheckPoint read: " + checkPoint.getRight().size() + " elements with key: " + key);

            if(checkPoint.getRight().size() == 0){
                Operator operator = CreateOperator.createOperator(reduceMessage.getReduce().getLeft(), reduceMessage.getReduce().getRight());
                List<KeyValuePair> data = hadoopWorker.readKey(reduceMessage.getProgramId(),key);
                logger.info(Thread.currentThread().getName()+ ": Data read from HDFS: " + data.size() + " elements with key: " + key);
                temp.addAll(operator.execute(data));
                i++;
            }else{
                logger.info(Thread.currentThread().getName() + ": Checkpoint exists for key: " + key);
                result.addAll(checkPoint.getRight());
                j++;
            }

            if((i%sizeCheckPoint == 0 || (i+j) == reduceMessage.getKeys().size()) && temp.size() > 0){
                logger.info(Thread.currentThread().getName()+ ": Writing checkpoint, elements: " + temp);
                checkPointManager.writeCheckPointPhase2(temp,reduceMessage.getProgramId(),true);
                logger.info(Thread.currentThread().getName() + ": CheckPoint written: " + temp.size() + " elements");
                result.addAll(temp);
                temp.clear();
            }
            
        }
        logger.info(Thread.currentThread().getName() + ": Reduce done with result: " + result);
        return result;
    }

    private List<KeyValuePair> processTask(Task task) throws IOException{
        Thread.currentThread().setName(Thread.currentThread().getName() + ":" + task.getProgramId());

        logger.info(Thread.currentThread().getName() +": Processing task: " + task.getTaskId());

        List<KeyValuePair> result = null;
                        
        logger.info(Thread.currentThread().getName() + ": Check if a checkpoint exists for task " + task.getTaskId());
        MutablePair<Boolean, List<KeyValuePair>> checkPoint = checkPointManager.checkCheckPoint(task.getTaskId(),task.getProgramId(),false);
        logger.info(Thread.currentThread().getName() + ": CheckPoint : " + checkPoint.getRight().size() + " elements");
        
        Integer size = checkPoint.getRight().size();
        result = checkPoint.getRight();
        Boolean finished = checkPoint.getLeft();

   
        

        List<KeyValuePair> tempResult = new ArrayList<>();
        if(!finished){

            List<Operator> operators = handleOperators(task.getOperators());
            logger.info(Thread.currentThread().getName() +": Operators: " + operators.size() + " elements");

            logger.info(Thread.currentThread().getName() +": No checkpoint found for task " + task.getTaskId() + ", processing the task");

            List<KeyValuePair> data = hadoopWorker.readInputFile(task.getPathFile());
            
            Integer sizeCheckPoint = 0;

            if(data.size() > 10){
                sizeCheckPoint = data.size()/10;
            }else{
                sizeCheckPoint = data.size();
            }
            logger.info(Thread.currentThread().getName() +": Checkpointing every " + sizeCheckPoint + " elements");
            logger.info(Thread.currentThread().getName() +": Data read from HDFS: " + data.size() + " elements");
            
            for(int i = size; i < data.size();){

                Integer end = i+sizeCheckPoint; 
                if(i+sizeCheckPoint > data.size()){
                    end = data.size();
                }
                for (int k = 0;k < operators.size(); k++) {
                    if(k == 0){
                        tempResult = (operators.get(k).execute(data.subList(i, end)));
                        i = end;
                    }else{
                        tempResult = (operators.get(k).execute(tempResult));
                    }
                }
                result.addAll(tempResult);
                logger.info(Thread.currentThread().getName() +": Writing checkpoint, elements: " + result.size() + " for task " + task.getTaskId());
                checkPointManager.writeCheckPointPhase1(task.getTaskId(),task.getProgramId(), result,false);
                logger.info(Thread.currentThread().getName() +": CheckPoint written: " + result.size() + " elements" + " for task " + task.getTaskId()); 
            }

            for(Operator o : operators){
                if(o instanceof ReduceOperator){
                    result = o.execute(result);
                }   
            }
            logger.info(Thread.currentThread().getName() + ": Writing last checkpoint, elements: " + result.size() + " for task " + task.getTaskId());   
            checkPointManager.writeCheckPointPhase1(task.getTaskId(),task.getProgramId(), result,true);
            logger.info(Thread.currentThread().getName() +": Last checkpoint written: " + result.size() + " elements" + " for task " + task.getTaskId()); 
        }
        logger.info(Thread.currentThread().getName() +": Task " + task.getTaskId() + " done" + " with result:" + result);
        return result;
    }

    public List<Integer> extractKeys(List<KeyValuePair> keyValuePairs) {
        List<Integer> keys = new ArrayList<>();

        for (KeyValuePair pair : keyValuePairs) {
            keys.add(pair.getKey());
        }
        logger.info(Thread.currentThread().getName() +": Keys extracted: " + keys);
        return keys;
    }
}