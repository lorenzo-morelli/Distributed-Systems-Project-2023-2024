package it.polimi.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.ConfigFileReader;
import it.polimi.common.KeyValuePair;

public class CheckPointReaderWriter {

    private static final String OUTPUT_DIRECTORY = "checkpoints/";
   
    public static MutablePair<Boolean, List<KeyValuePair>> checkCheckPoint(Integer taskId) throws Exception {
        
        String fileName = OUTPUT_DIRECTORY+"task" + taskId + ".json";
        File file = new File(fileName);
        MutablePair<Boolean, List<KeyValuePair>> result = new MutablePair<>(false, new ArrayList<>());
        
        if(file.exists()){
            result = ConfigFileReader.readCheckPoint(file);
        }
        return result;
    }



    public static void writeCheckPoint(Integer taskId, List<KeyValuePair> result, boolean finished) {
        String fileName = OUTPUT_DIRECTORY + "task" + taskId + ".json";
        
        createCheckpoint(result, fileName,finished);
        System.out.println("Checkpoint created for task " + taskId);
        
    }

     private synchronized static void createCheckpoint(List<KeyValuePair> result, String fileName,boolean finished) {
        createOutputDirectory(); // Ensure the 'OUTPUT_DIRECTORY' directory exists

        try{
            ConfigFileReader.createCheckpoint(result, fileName,finished);
        }
        catch(Exception e){
            System.out.println("Error while writing the checkpoint");
            System.out.println(e.getMessage());   
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
