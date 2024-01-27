package it.polimi.worker;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.KeyValuePair;
import it.polimi.common.Messages.Heartbeat;
import it.polimi.common.Messages.Task;
import it.polimi.common.Operators.Operator;

class WorkerHandler extends Thread {
    private Socket clientSocket;

    public WorkerHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            // Create input and output streams for communication
            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());

            // Read the object from the coordinator
            Object object = inputStream.readObject();

            if (object instanceof Task) {
                Task task = (Task) object;
                // Process the Task
                List<KeyValuePair> result = processTask(task);
                // Send the result back to the coordinator
                outputStream.writeObject(result);
            } else if (object instanceof Heartbeat) {
                System.out.println("Heartbeat received");
                // Send the result back to the coordinator
                outputStream.writeObject(new Heartbeat());
            } else {
                // Handle other types or unexpected objects
                 System.out.println("Received unexpected object type");
            }
        
            // Close the streams and socket when done
            inputStream.close();
            outputStream.close();
            clientSocket.close();
        } catch (Exception e) {
            System.out.println("I can read");

            e.printStackTrace();
        }
    }

    private List<Operator> handleOperators(List<MutablePair<String,String>> dataFunctions){
        List<Operator> operators = new ArrayList<>();
        for (MutablePair<String,String> df: dataFunctions) {
            String op = df.getLeft(); 
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
        }
        return operators;
    }
    

    private List<KeyValuePair> processTask(Task task){        
        List<Operator> operators = handleOperators(task.getOperators());
        
        List<KeyValuePair> result = operators.get(0).execute(task.getData());
        operators.remove(0);
            
        for(Operator o: operators){
            result = o.execute(result);
        }

        return result;
    }   
    

}