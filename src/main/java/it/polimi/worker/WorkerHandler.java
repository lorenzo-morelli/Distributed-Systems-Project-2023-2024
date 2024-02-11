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
import it.polimi.common.messages.Heartbeat;
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
                    // Process the Task
                    List<KeyValuePair> result = processTask(task);
                    
                    if (result != null) {
                        if(!task.isPresentStep2()){
                            // Send the result back to the coordinator
                            outputStream.writeObject(result);
                            break;
                        }else{
                            HadoopFileReadWrite.writeKeys(
                                taskId.toString(),
                                result
                                );
                            outputStream.writeObject(extractKeys(result));

                        }
                    }else{
                        outputStream.writeObject(new ErrorMessage());
                    }
                } else if (object instanceof Heartbeat) {
                    System.out.println("Heartbeat received");
                    // Send the result back to the coordinator
                    outputStream.writeObject(new Heartbeat());
                } else if (object instanceof LastReduce){
                    LastReduce reduceMessage = (LastReduce) object;

                    System.out.println("Responsible for the keys: " + reduceMessage.getKeys());
                    
                    outputStream.writeObject(computeReduceMessage(reduceMessage));
                    break;
                }
                else {
                    // Handle other types or unexpected objects
                    System.out.println("Received unexpected object type");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Connection closed");
        } finally {
            System.out.println("Closing connection");
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
                e.printStackTrace();
            }
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
    private List<KeyValuePair> computeReduceMessage(LastReduce reduceMessage){

        List<KeyValuePair> data = HadoopFileReadWrite.readKeys(reduceMessage.getKeys());
        Operator operator = CreateOperator.createOperator(reduceMessage.getReduce().getLeft(), reduceMessage.getReduce().getRight());

        List<KeyValuePair> result = operator.execute(data);
        return result;
    }

    private List<KeyValuePair> processTask(Task task) {
        
        List<KeyValuePair> result = null;
        try {
                        
            List<KeyValuePair> data = HadoopFileReadWrite.readInputFile(task.getPathFile());
            
            List<KeyValuePair> checkPoint = CheckPointReaderWriter.checkCheckPoint(task.getTaskId());
            
            Integer size = checkPoint.size();
            result = checkPoint;

            List<Operator> operators = handleOperators(task.getOperators());
            Integer sizeCheckPoint = 8;

            List<KeyValuePair> tempResult = new ArrayList<>();

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
                CheckPointReaderWriter.writeCheckPoint(task.getTaskId(), result,i);
            }

            for(Operator o : operators){
                if(o instanceof ReduceOperator){
                    result = o.execute(result);
                }   
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
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