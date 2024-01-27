package it.polimi.worker;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;

import it.polimi.common.Heartbeat;
import it.polimi.common.KeyValuePair;
import it.polimi.common.Operator;
import it.polimi.common.Task;

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
            e.printStackTrace();
        }
    }
    private List<KeyValuePair> processTask(Task task){        
        
        List<KeyValuePair> result = task.getOperators().get(0).execute(task.getData());
        task.getOperators().remove(0);
            
        for(Operator o: task.getOperators()){
            result = o.execute(result);
        }

        return result; 
    }   
    

}