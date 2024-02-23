package it.polimi.worker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.file.Paths;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.shaded.json.JSONObject;

import it.polimi.common.KeyValuePair;

public class CheckPointManager {

    private final String OUTPUT_DIRECTORY = "checkpoints-";
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private static final ReentrantLock folderLock = new ReentrantLock();

    public MutablePair<Boolean, List<KeyValuePair>> checkCheckPoint(Integer taskId, String programId, Boolean phase2){
        String fileName;

        if(phase2){
            fileName = "key" + taskId + ".json";
        }else{
            fileName = "task" + taskId + ".json";
        }

        String path = OUTPUT_DIRECTORY+programId+"/"+ fileName;
        File file = new File(path);
        MutablePair<Boolean, List<KeyValuePair>> result = new MutablePair<>(false, new ArrayList<>());
        
        
        logger.info(Thread.currentThread().getName() +": Check if checkpoint exists :" + path);
        if(file.exists()){
            try{
                result = readCheckPoint(file,phase2);
                logger.info(Thread.currentThread().getName() + ": Checkpoint found for " + path);
            }catch(IOException e){
                logger.warn(Thread.currentThread().getName() + ": Error while reading the checkpoint");
                System.out.println(Thread.currentThread().getName() + " :" + e.getMessage());
            }
        }
        return result;
    }
    
    public void writeCheckPointPhase1(Integer taskId,String programId, List<KeyValuePair> result, boolean finished) {
        String fileName = OUTPUT_DIRECTORY+programId+ "/task" + taskId + ".json";
        
        logger.info(Thread.currentThread().getName() + ": Creating checkpoint phase 1 for task " + taskId);
        createCheckpoint(result, fileName,finished,false,programId);
        logger.info(Thread.currentThread().getName() +": Checkpoint phase 1 created for task " + taskId);
        
    }
    public void writeCheckPointPhase2(List<KeyValuePair> result,String programId, boolean finished) {
        
        ArrayList<KeyValuePair> temp = new ArrayList<>();      
        for(KeyValuePair k : result){
            String fileName = OUTPUT_DIRECTORY+programId + "/key" + k.getKey() + ".json";
            temp.add(k);
            logger.info(Thread.currentThread().getName() + ": Creating checkpoint phase 2" + " for key " + k.getKey());
            createCheckpoint(temp, fileName,finished,true,programId);
            logger.info(Thread.currentThread().getName() +": Checkpoint created for phase 2" + " for key " + k.getKey());
            temp.remove(k);
        }        
    }
   
    private void createCheckpoint(List<KeyValuePair> result, String fileName,boolean finished, boolean phase2, String programId) {
        CheckPointManager.createOutputDirectory(OUTPUT_DIRECTORY+programId+"/");

        try{
            logger.info(Thread.currentThread().getName() + ": Writing the checkpoint to file " + fileName);
            writeCheckPoint(result, fileName,finished,phase2);
            logger.info(Thread.currentThread().getName() + ": Checkpoint written to file " + fileName);
        }
        catch(IOException e){
            logger.error(Thread.currentThread().getName() + ": Error while writing the checkpoint");
            System.out.println(Thread.currentThread().getName() + ": Error while writing the checkpoint");
            System.out.println(e.getMessage());
        }
    }

    
    public MutablePair<Boolean, List<KeyValuePair>> readCheckPoint(File file, Boolean phase2) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Reading checkpoint file: " + file.getAbsolutePath().toString());
        List<KeyValuePair> result = new ArrayList<>();
        Boolean end = false;
        
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
    
            result = objectMapper.convertValue(jsonData.get("values"), new TypeReference<List<KeyValuePair>>() {});    
            if(!phase2){
                end = (Boolean) jsonData.get("end");
            }
        } catch (IOException e) {
            logger.warn(e);
            throw new IOException("Not possible to read the checkpoint file:\n" + file.getAbsolutePath().toString());
        }
        logger.info(Thread.currentThread().getName() + ": Checkpoint file read: " + file.getAbsolutePath().toString());
        return new MutablePair<>(end, result);
    }
    public void writeCheckPoint(List<KeyValuePair> result, String fileName, boolean finished, boolean phase2) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Creating checkpoint file: " + fileName);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("values", result);
    
        if (!phase2) {
            jsonObject.put("end", finished);
        }
        
        String tempFileName = fileName.split("\\.")[0] + "_temp.json";

        logger.info(Thread.currentThread().getName() + ": Writing temp checkpoint file: " + tempFileName);
       // Write the JSON object to a file
        try (FileWriter fileWriter = new FileWriter(tempFileName)) {
            fileWriter.write(jsonObject.toJSONString());
        } catch (IOException e) {
            logger.error(e);
            throw new IOException("Not possible to write the checkpoint file:\n" + tempFileName);
        }
        logger.info(Thread.currentThread().getName() + ": Temp checkpoint file written: " + tempFileName);

        Path sourcePath = Path.of(tempFileName);
        Path destinationPath = Path.of(fileName);
        logger.info(Thread.currentThread().getName() + ": Moving temp checkpoint file to: " + fileName);
        Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
        logger.info(Thread.currentThread().getName() + ": Temp checkpoint file moved to: " + fileName);
        Files.deleteIfExists(sourcePath);
        logger.info(Thread.currentThread().getName() + ": Temp checkpoint file deleted: " + tempFileName);
    }
    private static void createOutputDirectory(String OUTPUT_DIRECTORY) {
        try{
            folderLock.lock();

            Path outputDirectoryPath = Paths.get(OUTPUT_DIRECTORY);

            if (Files.notExists(outputDirectoryPath)) {
                try {
                    Files.createDirectories(outputDirectoryPath);
                    logger.info(Thread.currentThread().getName() + ": Created '"+outputDirectoryPath+"' directory.");
                } catch (IOException e) {
                    logger.error(Thread.currentThread().getName()+ ": Error while creating '"+outputDirectoryPath+"' directory");
                    System.out.println(Thread.currentThread().getName() + ": Error while creating '"+outputDirectoryPath+"' directory");
                    System.out.println(e.getMessage());
                }
            }
        }finally{
            folderLock.unlock();
        }
    }

    public void deleteCheckPoint(Integer id, String programId, boolean phase2) {
        String fileName;
        if(phase2){
            fileName = OUTPUT_DIRECTORY+programId+"/key" + id + ".json";
        }else{
            fileName = OUTPUT_DIRECTORY+programId+"/task" + id + ".json";
        }
        File file = new File(fileName);
        if(file.exists()){
            file.delete();
        }
        deleteEmptyDirectory(new File(OUTPUT_DIRECTORY+programId+"/"));
    }
    private static void deleteEmptyDirectory(File folder) {
        try{
            folderLock.lock();
            if (folder.exists() && folder.isDirectory() && folder.list().length == 0) {
                folder.delete();
            }
        }finally{
            folderLock.unlock();
        }
    }
}
