package it.polimi.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private String OUTPUT_DIRECTORY_1 = "step1";
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
            OUTPUT_DIRECTORY_1 = "step1_" + clientSocket.getLocalPort();
            while (true) {

                // Read the object from the coordinator
                Object object = inputStream.readObject();
                if (object instanceof Task) {
                    Task task = (Task) object;

                    // Process the Task
                    List<KeyValuePair> result = processTask(task);
                    
                    if (result != null) {
                        if(!task.isPresentStep2()){
                            // Send the result back to the coordinator
                            outputStream.writeObject(result);
                        }else{

                            createFilesForStep2(result);
                            outputStream.writeObject(extractKeys(result));

                        }
                    } else {
                        outputStream.writeObject(new ErrorMessage());
                    }

                } else if (object instanceof Heartbeat) {
                    System.out.println("Heartbeat received");
                    // Send the result back to the coordinator
                    outputStream.writeObject(new Heartbeat());
                } else if(object instanceof List<?>){
                    System.out.println(object);
                }
                else {
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

    public static List<Integer> extractKeys(List<KeyValuePair> keyValuePairs) {
        List<Integer> keys = new ArrayList<>();

        for (KeyValuePair pair : keyValuePairs) {
            keys.add(pair.getKey());
        }

        return keys;
    }


    private void createFilesForStep2(List<KeyValuePair> result){
        createOutputDirectory(); // Ensure the 'output' directory exists

        for (KeyValuePair pair : result) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            String fileName = OUTPUT_DIRECTORY_1 + "/file_" + key + ".csv"; // Save files in the 'output' directory

            try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {

                fileOutputStream.write((key + "," + value + "\n").getBytes());

                System.out.println("File created for key " + key + ": " + fileName);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void createOutputDirectory() {
        Path outputDirectoryPath = Paths.get(OUTPUT_DIRECTORY_1);

        if (Files.notExists(outputDirectoryPath)) {
            try {
                Files.createDirectories(outputDirectoryPath);
                System.out.println("Created 'output' directory.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}