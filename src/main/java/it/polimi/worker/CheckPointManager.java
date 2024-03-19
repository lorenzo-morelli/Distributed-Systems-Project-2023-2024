package it.polimi.worker;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.worker.utils.CheckpointInfo;

import java.io.File;

public class CheckPointManager {

    private final String CHECKPOINT_DIRECTORY = "checkpoints-";
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private static final ReentrantLock folderLock = new ReentrantLock();
    private final Set<String> createdFiles = new HashSet<>();

    public void createCheckpoint(String programId, String pathString,CheckpointInfo checkPointObj){
        createOutputDirectory(CHECKPOINT_DIRECTORY + programId);
        try {
            
            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            createdFiles.add(pathString);
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathString, true));
            writer.write("<Checkpoint:" + checkPointObj.getCount() + "><"+checkPointObj.getEnd()+"><"+checkPointObj.getRemainingString()+">\n");
            writer.close();
            logger.info(Thread.currentThread().getName() + ": Created checkpoint file " + pathString);
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(e.getMessage());
        }
    }
    public CheckpointInfo getCheckPoint(String programId,String pathString) {
        int count = 0;
        boolean end = false;
        String remainingString = "";
        try {
            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();   
            if (!Files.exists(Paths.get(pathString))) {
                logger.warn(Thread.currentThread().getName() + ": Checkpoint file " + pathString + " does not exist");
                return new CheckpointInfo(0, false,"");
            }
            BufferedReader reader = new BufferedReader(new FileReader(pathString));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("<Checkpoint")) {
                    String[] parts = line.split("><");
                    if (parts.length != 3) {
                        reader.close();
                        throw new NumberFormatException("Invalid checkpoint format");
                    }
                    try{
                        int temp_count= Integer.parseInt(parts[0].split(":")[1]);
                        boolean temp_end= Boolean.parseBoolean(parts[1].substring(0, parts[1].length()));
                        String  temp_remainingString;
                       
                        if(parts[2].length() > 1){
                            temp_remainingString= parts[2].substring(0, parts[2].length()-1);
                        }else{
                            temp_remainingString="";
                        }                    
                        count = temp_count;
                        end = temp_end;
                        remainingString = temp_remainingString;
                    }catch(NumberFormatException e){
                        reader.close();
                        throw new NumberFormatException("Invalid checkpoint format");
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while reading checkpoint file");
        } catch (NumberFormatException e) {
            logger.warn(Thread.currentThread().getName() +": "+ e.getMessage());
        }
        logger.info(Thread.currentThread().getName() + ": Retrieved checkpoint file " + pathString + " with count " + count + " and end " + end + " and remaining string " + remainingString);
        return new CheckpointInfo(count, end, remainingString);
}


    private static void createOutputDirectory(String directory) {
        try{
            folderLock.lock();
            Path outputDirectoryPath = Paths.get(directory);

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
        }catch(Exception e){
            logger.error(Thread.currentThread().getName() + ": Error while creating '"+directory+"' directory");
            System.out.println(Thread.currentThread().getName() + ": Error while creating '"+directory+"' directory");
            System.out.println(e.getMessage());
        }
        finally{
            folderLock.unlock();
        }
    }

    public void deleteCheckpoints(String programId){
        try{
            for(String file : createdFiles){
                Path path = Paths.get(file);
                Files.deleteIfExists(path);
                logger.info(Thread.currentThread().getName() + ": Deleted checkpoint file " + file);
            }
            deleteEmptyDirectory(new File(CHECKPOINT_DIRECTORY+programId+"/"));
        }catch(Exception e){
            logger.error(Thread.currentThread().getName() + ": Error while deleting checkpoints");
            System.out.println(Thread.currentThread().getName() + ": Error while deleting checkpoints");
        }
    }
    private static void deleteEmptyDirectory(File folder) {
        try{
            folderLock.lock();
            if (folder.exists() && folder.isDirectory() && folder.list().length == 0) {
                logger.info(Thread.currentThread().getName() + ": Deleting '"+folder.getName()+"' directory");
                folder.delete();
            }
        }catch(Exception e){
            logger.error(Thread.currentThread().getName() + ": Error while deleting '"+folder.getName()+"' directory");
            System.out.println(Thread.currentThread().getName() + ": Error while deleting '"+folder.getName()+"' directory");
        }
        finally{
            folderLock.unlock();
        }
    }
   
}
