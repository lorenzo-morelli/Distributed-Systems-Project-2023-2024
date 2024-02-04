package it.polimi.worker;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.ConfigFileReader;
import it.polimi.common.KeyValuePair;
import it.polimi.common.Operator;
import it.polimi.common.messages.ErrorMessage;
import it.polimi.common.messages.Heartbeat;
import it.polimi.common.messages.Task;

class WorkerHandler extends Thread {
    private Socket clientSocket;

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

                    // Process the Task
                    List<KeyValuePair> result = processTask(task);

                    if (result != null) {
                        // Send the result back to the coordinator
                        outputStream.writeObject(result);
                    } else {
                        outputStream.writeObject(new ErrorMessage());
                    }

                } else if (object instanceof Heartbeat) {
                    System.out.println("Heartbeat received");
                    // Send the result back to the coordinator
                    outputStream.writeObject(new Heartbeat());
                } else {
                    // Handle other types or unexpected objects
                    System.out.println("Received unexpected object type");
                }
            }
        } catch (Exception e) {
            System.out.println("Connection closed");
        } finally {
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


    private List<KeyValuePair> processTask(Task task) {
        
        List<KeyValuePair> result = null;
        try {
            List<KeyValuePair> data = ConfigFileReader.readData(new File(task.getPathFile()));
            List<Operator> operators = handleOperators(task.getOperators());

            result = operators.get(0).execute(data);
            operators.remove(0);

            for (Operator o : operators) {
                result = o.execute(result);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return result;
    }


}