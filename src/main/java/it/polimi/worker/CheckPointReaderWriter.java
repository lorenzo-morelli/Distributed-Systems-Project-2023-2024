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
   
    public static MutablePair<Boolean, List<KeyValuePair>> checkCheckPoint(Integer integer, Boolean phase2) throws Exception {
        String fileName;

        if(phase2){
            fileName = "key" + integer + ".json";
        }else{
            fileName = "task" + integer + ".json";
        }

        String path = OUTPUT_DIRECTORY+ fileName;
        File file = new File(path);
        MutablePair<Boolean, List<KeyValuePair>> result = new MutablePair<>(false, new ArrayList<>());
        
        if(file.exists()){
            try{
                result = ConfigFileReader.readCheckPoint(file,phase2);
            }catch(Exception e){
                System.out.println(e.getMessage());
            }
        }
        return result;
    }

    public static void writeCheckPointPhase1(Integer taskId, List<KeyValuePair> result, boolean finished) {
        String fileName = OUTPUT_DIRECTORY + "task" + taskId + ".json";
        
        createCheckpoint(result, fileName,finished,false);
        System.out.println("Checkpoint created for task " + taskId);
        
    }
    public static void writeCheckPointPhase2(List<KeyValuePair> result, boolean finished) {
        ArrayList<KeyValuePair> temp = new ArrayList<>();      
        for(KeyValuePair k : result){
            String fileName = OUTPUT_DIRECTORY + "key" + k.getKey() + ".json";
            temp.add(k);
            createCheckpoint(temp, fileName,finished,true);
            temp.remove(k);
        }        
    }
   
    private synchronized static void createCheckpoint(List<KeyValuePair> result, String fileName,boolean finished, boolean phase2) {
        createOutputDirectory(); // Ensure the 'OUTPUT_DIRECTORY' directory exists

        try{
            ConfigFileReader.createCheckpoint(result, fileName,finished,phase2);
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
