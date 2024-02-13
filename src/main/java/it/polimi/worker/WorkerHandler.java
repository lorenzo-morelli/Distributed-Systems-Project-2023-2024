package it.polimi.worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;

import it.polimi.common.HadoopFileReadWrite;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.LastReduce;
import it.polimi.common.messages.Task;
import it.polimi.worker.operators.ReduceOperator;

class WorkerHandler extends Thread {
    private Socket clientSocket;
    private Integer taskId;
    private final Logger logger;
    public WorkerHandler(Socket clientSocket, Logger logger) {
        this.clientSocket = clientSocket;
        this.logger = logger;
    }

    @Override
    public void run() {
        String socketAddress = clientSocket.getInetAddress().getHostName() +":"+ clientSocket.getPort() + ": ";
        logger.info(socketAddress + "WorkerHandler started.");

        System.out.println("WorkerHandler started");
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;

        try {

            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            while (true) {

                // Read the object from the coordinator
                Object object = inputStream.readObject();
                logger.info(socketAddress+ "Received object from coordinator: " + object);
                if (object instanceof Task) {
                    Task task = (Task) object;
                    taskId = task.getTaskId(); 
                    List<KeyValuePair> result  = new ArrayList<>();
                    
                    try{
                        // Process the Task
                        result = processTask(task);
                    }
                    catch(IOException e){
                        logger.error(socketAddress + "Error while processing the task: " + e.getMessage());
                        outputStream.writeObject(new ErrorMessage(e.getMessage()));
                        System.out.println("Error while processing the task");
                        break;
                    }

                    if (result != null) {
                        if(!task.isPresentStep2()){
                            logger.info(socketAddress+ "Step 1 is done, sending result back to the coordinator: " + result);
                            // Send the result back to the coordinator
                            outputStream.writeObject(result);
                            break;
                        }else{
                            try{
                                logger.info(socketAddress + "Writing the keys in HDFS");
                                HadoopFileReadWrite.writeKeys(
                                    taskId.toString(),
                                    result
                                    );
                            }catch(IOException e){
                                logger.error(socketAddress+ "Error while writing the keys in HDFS: " + e.getMessage());
                                outputStream.writeObject(new ErrorMessage("Error while writing the keys in HDFS"));
                                break;
                            }
                            outputStream.writeObject(extractKeys(result));
                            logger.info(socketAddress + "Keys sent to the coordinator" + extractKeys(result));

                        }
                    }else{
                        outputStream.writeObject(new ErrorMessage("Result is null!"));
                        logger.error(socketAddress + "Result is null!");
                        break;
                    }
                } else if (object instanceof LastReduce){
                    LastReduce reduceMessage = (LastReduce) object;

                    logger.info(socketAddress + "Received LastReduce message from coordinator, responsible for the keys: " + reduceMessage.getKeys());
                    System.out.println("Responsible for the keys: " + reduceMessage.getKeys());
                    
                    List<KeyValuePair> result = new ArrayList<>(); 
                    try{

                        result = computeReduceMessage(reduceMessage);
                    }
                    catch(IOException e){
                        outputStream.writeObject(new ErrorMessage("Error in the reduce phase"));
                        logger.error(socketAddress + "Error in the reduce phase: " + e.getMessage());
                        break;
                    }
                    outputStream.writeObject(result);
                    logger.info(socketAddress + result + " sent to the coordinator");
                    break;
                }
                else {
                    // Handle other types or unexpected objects
                    System.out.println("Received unexpected object type");
                    outputStream.writeObject(new ErrorMessage("Received unexpected object type"));
                    logger.error(socketAddress + "Received unexpected object type");
                    break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Coordinator connection lost");
            logger.error("Coordinator connection lost: " + e.getMessage());
        } finally {
            System.out.println("Closing connection");
            logger.info("Closing connection");
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
                logger.error("Error while closing the connection: " + e.getMessage());
                System.out.println(e.getMessage());
            }
        }
    }

    private List<Operator> handleOperators(List<MutablePair<String, String>> dataFunctions) {
        logger.info("Handling operators");
        List<Operator> operators = new ArrayList<>();

        for (MutablePair<String, String> df : dataFunctions) {
            String op = df.getLeft();
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
        }


        return operators;
    }
    private List<KeyValuePair> computeReduceMessage(LastReduce reduceMessage) throws IOException{
        logger.info("Computing reduce message");
        List<KeyValuePair> result = new ArrayList<>();
        List<KeyValuePair> temp = new ArrayList<>();

        Integer sizeCheckPoint = 2;
        Integer i = 0;
        Integer j = 0;
        for(Integer key: reduceMessage.getKeys()){

            List<KeyValuePair> data = HadoopFileReadWrite.readKey(key);
            
            MutablePair<Boolean, List<KeyValuePair>> checkPoint = CheckPointReaderWriter.checkCheckPoint(key,true);
            
            if(checkPoint.getRight().size() == 0){
                Operator operator = CreateOperator.createOperator(reduceMessage.getReduce().getLeft(), reduceMessage.getReduce().getRight());
                temp.addAll(operator.execute(data));
                i++;
            }else{
                result.addAll(checkPoint.getRight());
                j++;
            }

            if(i%sizeCheckPoint == 0 || (i+j) == reduceMessage.getKeys().size() || temp.size() > 0){
                logger.info("Writing checkpoint, elements: " + temp);
                CheckPointReaderWriter.writeCheckPointPhase2(temp,true);
                result.addAll(temp);
                temp.clear();
            }
            
        }
        return result;
    }

    private List<KeyValuePair> processTask(Task task) throws IOException{
        logger.info("Processing task: " + task.getTaskId());

        List<KeyValuePair> result = null;
                        
        List<KeyValuePair> data = HadoopFileReadWrite.readInputFile(task.getPathFile());
        logger.info("Data read from HDFS: " + data.size() + " elements");
        MutablePair<Boolean, List<KeyValuePair>> checkPoint = CheckPointReaderWriter.checkCheckPoint(task.getTaskId(),false);
        logger.info("CheckPoint read: " + checkPoint.getRight().size() + " elements");
        
        Integer size = checkPoint.getRight().size();
        result = checkPoint.getRight();
        Boolean finished = checkPoint.getLeft();

        List<Operator> operators = handleOperators(task.getOperators());
        logger.info("Operators: " + operators.size() + " elements");
        Integer sizeCheckPoint = 8;

        List<KeyValuePair> tempResult = new ArrayList<>();
        if(!finished){  
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
                CheckPointReaderWriter.writeCheckPointPhase1(task.getTaskId(), result,false);
                logger.info("CheckPoint written: " + result.size() + " elements" + " for task " + task.getTaskId()); 
            }

            for(Operator o : operators){
                if(o instanceof ReduceOperator){
                    result = o.execute(result);
                }   
            }
            CheckPointReaderWriter.writeCheckPointPhase1(task.getTaskId(), result,true);
            logger.info("Last checkpoint written: " + result.size() + " elements" + " for task " + task.getTaskId()); 
        }
        logger.info("Task " + task.getTaskId() + " done" + " with result:" + result);
        return result;
    }

    public static List<Integer> extractKeys(List<KeyValuePair> keyValuePairs) {
        List<Integer> keys = new ArrayList<>();

        for (KeyValuePair pair : keyValuePairs) {
            keys.add(pair.getKey());
        }

        return keys;
    }
}