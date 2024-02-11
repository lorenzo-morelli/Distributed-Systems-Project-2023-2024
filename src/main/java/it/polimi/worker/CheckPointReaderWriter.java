package it.polimi.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import it.polimi.common.ConfigFileReader;
import it.polimi.common.KeyValuePair;

public class CheckPointReaderWriter {

    private static final String OUTPUT_DIRECTORY = "checkpoints/";
   
    public static List<KeyValuePair> checkCheckPoint(Integer taskId) throws Exception {
        
        String fileName = OUTPUT_DIRECTORY+"task" + taskId + ".csv";
        File file = new File(fileName);
        List<KeyValuePair> keyValuePairs = new ArrayList<>();
        
        if(file.exists()){
            keyValuePairs = ConfigFileReader.readData(file);
        }
        return keyValuePairs;
    }



    public static void writeCheckPoint(Integer taskId, List<KeyValuePair> result, int size) {
        String fileName = OUTPUT_DIRECTORY + "task" + taskId + ".csv";
        
        createCheckpoint(result, fileName,size);
        
    }

     private synchronized static void createCheckpoint(List<KeyValuePair> result, String fileName,int size) {
        createOutputDirectory(); // Ensure the 'output' directory exists

        try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
            for(int j = 0;j<size;j++){
                KeyValuePair keyValuePair = result.get(j);
                fileOutputStream.write((keyValuePair.getKey() + "," + keyValuePair.getValue() + "\n").getBytes());
            }
            fileOutputStream.close();
        } catch (IOException e) {
                e.printStackTrace();
        }
    }

    private synchronized static void createOutputDirectory() {
        Path outputDirectoryPath = Paths.get(OUTPUT_DIRECTORY);

        if (Files.notExists(outputDirectoryPath)) {
            try {
                Files.createDirectories(outputDirectoryPath);
                System.out.println("Created 'checkpoints' directory.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
}
