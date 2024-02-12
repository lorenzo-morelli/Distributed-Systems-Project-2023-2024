package it.polimi.worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.HadoopFileReadWrite;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.LastReduce;
import it.polimi.common.messages.Task;
import it.polimi.worker.operators.ReduceOperator;

class WorkerHandler extends Thread {
    private Socket clientSocket;
    private Integer taskId;
    public WorkerHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        // Create input and output streams for communication
        ObjectInputStream inputStream = null;
        ObjectOutputStream outputStream = null;
        try {

            outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
            inputStream = new ObjectInputStream(clientSocket.getInputStream());
            while (true) {

                // Read the object from the coordinator
                Object object = inputStream.readObject();
                if (object instanceof Task) {
                    Task task = (Task) object;
                    taskId = task.getTaskId(); 
                    List<KeyValuePair> result  = new ArrayList<>();
                    
                    try{
                        // Process the Task
                        result = processTask(task);
                    }
                    catch(Exception e){
                        outputStream.writeObject(new ErrorMessage(e.getMessage()));
                        System.out.println("Error while processing the task");
                        break;
                    }

                    if (result != null) {
                        if(!task.isPresentStep2()){
                            // Send the result back to the coordinator
                            outputStream.writeObject(result);
                            break;
                        }else{
                            try{
                                HadoopFileReadWrite.writeKeys(
                                    taskId.toString(),
                                    result
                                    );
                            }catch(Exception e){
                                outputStream.writeObject(new ErrorMessage("Error while writing the keys in HDFS"));
                                break;
                            }
                            outputStream.writeObject(extractKeys(result));

                        }
                    }else{
                        outputStream.writeObject(new ErrorMessage("Result is null!"));
                    }
                } else if (object instanceof LastReduce){
                    LastReduce reduceMessage = (LastReduce) object;

                    System.out.println("Responsible for the keys: " + reduceMessage.getKeys());
                    
                    List<KeyValuePair> result = new ArrayList<>(); 
                    try{
                        result = computeReduceMessage(reduceMessage);
                    }
                    catch(Exception e){
                        outputStream.writeObject(new ErrorMessage("Error in the reduce phase"));
                        break;
                    }
                    outputStream.writeObject(result);
                    break;
                }
                else {
                    // Handle other types or unexpected objects
                    System.out.println("Received unexpected object type");
                    outputStream.writeObject(new ErrorMessage("Received unexpected object type"));
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Coordinator connection lost");
        } finally {
            System.out.println("Closing connection");
        }

    }

    private List<Operator> handleOperators(List<MutablePair<String, String>> dataFunctions) {
        List<Operator> operators = new ArrayList<>();

        for (MutablePair<String, String> df : dataFunctions) {
            String op = df.getLeft();
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
        }


        return operators;
    }
    private List<KeyValuePair> computeReduceMessage(LastReduce reduceMessage) throws IOException{
    
        List<KeyValuePair> data = HadoopFileReadWrite.readKeys(reduceMessage.getKeys());
        
        Operator operator = CreateOperator.createOperator(reduceMessage.getReduce().getLeft(), reduceMessage.getReduce().getRight());

        List<KeyValuePair> result = operator.execute(data);
        return result;
    }

    private List<KeyValuePair> processTask(Task task) throws Exception{
        
        List<KeyValuePair> result = null;
                        
        List<KeyValuePair> data = HadoopFileReadWrite.readInputFile(task.getPathFile());
        
        MutablePair<Boolean, List<KeyValuePair>> checkPoint = CheckPointReaderWriter.checkCheckPoint(task.getTaskId());
        
        Integer size = checkPoint.getRight().size();
        result = checkPoint.getRight();
        Boolean finished = checkPoint.getLeft();

        List<Operator> operators = handleOperators(task.getOperators());
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
                CheckPointReaderWriter.writeCheckPoint(task.getTaskId(), result,false);
            }

            for(Operator o : operators){
                if(o instanceof ReduceOperator){
                    result = o.execute(result);
                }   
            }
            CheckPointReaderWriter.writeCheckPoint(task.getTaskId(), result,true);
        }
     
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