package it.polimi.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.common.ConfigFileReader;
import it.polimi.common.KeyValuePair;

public class CheckPointReaderWriter {

    private static final String OUTPUT_DIRECTORY = "checkpoints/";
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");


    public static MutablePair<Boolean, List<KeyValuePair>> checkCheckPoint(Integer integer, Boolean phase2){
        String fileName;

        if(phase2){
            fileName = "key" + integer + ".json";
        }else{
            fileName = "task" + integer + ".json";
        }

        String path = OUTPUT_DIRECTORY+ fileName;
        File file = new File(path);
        MutablePair<Boolean, List<KeyValuePair>> result = new MutablePair<>(false, new ArrayList<>());
        
        
        logger.info(Thread.currentThread().getName() +": Check if checkpoint exists for " + fileName);
        if(file.exists()){
            try{
                result = ConfigFileReader.readCheckPoint(file,phase2);
                logger.info(Thread.currentThread().getName() + ": Checkpoint found for " + fileName);
            }catch(IOException e){
                logger.error(Thread.currentThread().getName() + ": Error while reading the checkpoint");
                System.out.println(e.getMessage());
            }
        }
        return result;
    }
    
    public static void writeCheckPointPhase1(Integer taskId, List<KeyValuePair> result, boolean finished) {
        String fileName = OUTPUT_DIRECTORY + "task" + taskId + ".json";
        
        logger.info(Thread.currentThread().getName() + ": Creating checkpoint phase 1 for task " + taskId);
        createCheckpoint(result, fileName,finished,false);
        logger.info(Thread.currentThread().getName() +": Checkpoint phase 1 created for task " + taskId);

        System.out.println("Checkpoint created for task " + taskId);
        
    }
    public static void writeCheckPointPhase2(List<KeyValuePair> result, boolean finished) {
        
        ArrayList<KeyValuePair> temp = new ArrayList<>();      
        for(KeyValuePair k : result){
            String fileName = OUTPUT_DIRECTORY + "key" + k.getKey() + ".json";
            temp.add(k);
            logger.info(Thread.currentThread().getName() + ": Creating checkpoint phase 2" + " for key " + k.getKey());
            createCheckpoint(temp, fileName,finished,true);
            logger.info(Thread.currentThread().getName() +": Checkpoint created for phase 2" + " for key " + k.getKey());
            temp.remove(k);
        }        
    }
   
    private synchronized static void createCheckpoint(List<KeyValuePair> result, String fileName,boolean finished, boolean phase2) {
        logger.info("Setting the output directory for the checkpoint file to 'checkpoints' directory.");
        createOutputDirectory(); // Ensure the 'OUTPUT_DIRECTORY' directory exists

        try{
            logger.info(Thread.currentThread().getName() + ": Writing the checkpoint to file " + fileName);
            ConfigFileReader.createCheckpoint(result, fileName,finished,phase2);
            logger.info(Thread.currentThread().getName() + ": Checkpoint written to file " + fileName);
        }
        catch(IOException e){
            logger.error(Thread.currentThread().getName() + ": Error while writing the checkpoint");
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
                System.out.println(e.getMessage());
            }
        }
    }
    
}
