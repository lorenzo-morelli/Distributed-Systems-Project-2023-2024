package it.polimi.worker;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.CheckpointInfo;

import java.io.File;
/**
 * The CheckPointManager class is used to manage the checkpoints.
 * It contains the methods to create, read and delete the checkpoints.
 * @param CHECKPOINT_DIRECTORY represents the directory where the checkpoints are stored.
 * @param logger represents the logger used to log the messages.
 * @param folderLock represents the lock used to synchronize the access to the directory.
 * @param createdFiles represents the set of the created files.
 * @return the methods to manage the checkpoints.
 * @see CheckpointInfo
 */
public class CheckPointManager {

    private final String CHECKPOINT_DIRECTORY = "checkpoints-";
    private static final Logger logger = LogManager.getLogger("it.polimi.Worker");
    private static final ReentrantLock folderLock = new ReentrantLock();
    private final Set<String> createdFiles = new HashSet<>();

    /**
     * The createCheckpoint method creates a new checkpoint file.
     * This method is invoked in the first phase when the program does not include a reduce operation unless it also includes a changekey operation.
     * It is called upon completion of processing a partition.
     * @param programId represents the program id.
     * @param pathString represents the path of the checkpoint file.
     * @param checkPointObj represents the checkpoint information.
     */
    public void createCheckpoint(String programId, String pathString, CheckpointInfo checkPointObj) {
        createOutputDirectory(CHECKPOINT_DIRECTORY + programId);
        try {

            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            createdFiles.add(pathString);
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathString, true));
            writer.write("<Checkpoint:" + checkPointObj.count() + "><" + checkPointObj.end() + "><" + checkPointObj.remainingString() + ">\n");
            writer.close();
            logger.info(Thread.currentThread().getName() + ": Created checkpoint file " + pathString + " with count " + checkPointObj.count() + ", end " + checkPointObj.end() + " and remaining string " + checkPointObj.remainingString());
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(e.getMessage());
        }
    }
    /**
     * The getCheckPoint method reads the checkpoint file.
     * It is called in the first phase before read the corresponding input file.
     * @param programId represents the program id.
     * @param pathString represents the path of the checkpoint file.
     * @return the checkpoint information.
     */
    public CheckpointInfo getCheckPoint(String programId, String pathString) {
        int count = 0;
        boolean end = false;
        String remainingString = "";
        try {
            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            if (!Files.exists(Paths.get(pathString))) {
                logger.info(Thread.currentThread().getName() + ": Checkpoint file " + pathString + " does not exist");
                return new CheckpointInfo(0, false, "", null);
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
                    try {
                        int temp_count = Integer.parseInt(parts[0].split(":")[1]);
                        boolean temp_end = Boolean.parseBoolean(parts[1]);
                        String temp_remainingString;

                        if (parts[2].length() > 1) {
                            temp_remainingString = parts[2].substring(0, parts[2].length() - 1);
                        } else {
                            temp_remainingString = "";
                        }
                        count = temp_count;
                        end = temp_end;
                        remainingString = temp_remainingString;
                    } catch (NumberFormatException e) {
                        reader.close();
                        throw new NumberFormatException("Invalid checkpoint format");
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while reading checkpoint file");
        } catch (NumberFormatException e) {
            logger.warn(Thread.currentThread().getName() + ": " + e.getMessage());
        }
        logger.info(Thread.currentThread().getName() + ": Retrieved checkpoint file " + pathString + " with count " + count + ", end " + end + " and remaining string " + remainingString);
        return new CheckpointInfo(count, end, remainingString, null);
    }
    /**
      * The createCheckpointForReduce method creates a new checkpoint file.
      * This method is invoked during the first phase when the program involves a reduce operation and does not include a changekey operation.
      * It is called upon completion of processing a partition.
      * @param programId represents the program id.
      * @param pathString represents the path of the checkpoint file.
      * @param checkPointObj represents the checkpoint information.
     */
    public void createCheckpointForReduce(String programId, String pathString, CheckpointInfo checkPointObj) {
        createOutputDirectory(CHECKPOINT_DIRECTORY + programId);
        try {

            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            createdFiles.add(pathString);
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathString, true));
            writer.write("<Checkpoint:" + checkPointObj.count() + "><" + checkPointObj.end() + "><" + checkPointObj.keyValuePair() + "><" + checkPointObj.remainingString() + ">\n");
            writer.close();
            logger.info(Thread.currentThread().getName() + ": Created checkpoint file " + pathString + " with count " + checkPointObj.count() + ", end " + checkPointObj.end() + ", remaining string " + checkPointObj.remainingString() + " and keyValuePair " + checkPointObj.keyValuePair());
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(e.getMessage());
        }
    }
    /**
     * The getCheckPointForReduce method reads the checkpoint file.
     * This method is invoked during the first phase when the program involves a reduce operation and does not include a changekey operation.
     * It is called in the first phase before read the corresponding input file.
     * @param programId represents the program id.
     * @param pathString represents the path of the checkpoint file.
     * @return the checkpoint information.
     */
    public CheckpointInfo getCheckPointForReduce(String programId, String pathString) {
        int count = 0;
        boolean end = false;
        String remainingString = "";
        KeyValuePair keyValuePair = null;
        try {
            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            if (!Files.exists(Paths.get(pathString))) {
                logger.info(Thread.currentThread().getName() + ": Checkpoint file " + pathString + " does not exist");
                return new CheckpointInfo(0, false, "", null);
            }
            BufferedReader reader = new BufferedReader(new FileReader(pathString));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("<Checkpoint")) {
                    String[] parts = line.split("><");
                    if (parts.length != 4) {
                        reader.close();
                        throw new NumberFormatException("Invalid checkpoint format");
                    }
                    try {
                        int temp_count = Integer.parseInt(parts[0].split(":")[1]);
                        boolean temp_end = Boolean.parseBoolean(parts[1]);
                        String temp_remainingString;
                        String[] keyValueString = parts[2].split(",");
                        if (parts[3].length() > 1) {
                            temp_remainingString = parts[3].substring(0, parts[3].length() - 1);
                        } else {
                            temp_remainingString = "";
                        }
                        count = temp_count;
                        end = temp_end;
                        remainingString = temp_remainingString;
                        keyValuePair = new KeyValuePair(Integer.parseInt(keyValueString[0]), Integer.parseInt(keyValueString[1]));
                    } catch (NumberFormatException e) {
                        reader.close();
                        throw new NumberFormatException("Invalid checkpoint format");
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while reading checkpoint file");
        } catch (NumberFormatException e) {
            logger.warn(Thread.currentThread().getName() + ": " + e.getMessage());
        }
        logger.info(Thread.currentThread().getName() + ": Retrieved checkpoint file " + pathString + " with count " + count + ", end " + end + ", remaining string " + remainingString + " and keyValuePair " + keyValuePair);
        return new CheckpointInfo(count, end, remainingString, keyValuePair);
    }
    /**
     * The writeCheckPointReducePhase method creates a new checkpoint file.
     * This method is invoked during the reduce phase, i.e., the second phase.
     * It is called upon completion of processing a key.
     * @param programId represents the program id.
     * @param pathString represents the path of the checkpoint file.
     */
    public void writeCheckPointReducePhase(String programId, String pathString) {
        createOutputDirectory(CHECKPOINT_DIRECTORY + programId);
        try {

            Path path = Paths.get(pathString);
            pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
            createdFiles.add(pathString);
            BufferedWriter writer = new BufferedWriter(new FileWriter(pathString, true));
            writer.write("<Checkpoint done>\n");
            writer.close();
            logger.info(Thread.currentThread().getName() + ": Created checkpoint file " + pathString);
        } catch (IOException e) {
            logger.error(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(Thread.currentThread().getName() + ": Error while creating checkpoint file");
            System.out.println(e.getMessage());
        }
    }
    /**
     * The readCheckPointReducePhase method reads the checkpoint file.
     * This method is invoked during the reduce phase, i.e., the second phase.
     * It is called in the second phase before read the files for the corresponding key.
     * @param programId represents the program id.
     * @param pathString represents the path of the checkpoint file.
     * @return true if the checkpoint file exists, false otherwise.
     */
    public boolean readCheckPointReducePhase(String programId, String pathString) {
        Path path = Paths.get(pathString);
        pathString = CHECKPOINT_DIRECTORY + programId + "/" + path.getFileName();
        if (!Files.exists(Paths.get(pathString))) {
            logger.info(Thread.currentThread().getName() + ": Checkpoint file " + pathString + " does not exist");
            return false;
        } else {
            return true;
        }
    }


    private static void createOutputDirectory(String directory) {
        try {
            folderLock.lock();
            Path outputDirectoryPath = Paths.get(directory);

            if (Files.notExists(outputDirectoryPath)) {
                try {
                    Files.createDirectories(outputDirectoryPath);
                    logger.info(Thread.currentThread().getName() + ": Created '" + outputDirectoryPath + "' directory.");
                } catch (IOException e) {
                    logger.error(Thread.currentThread().getName() + ": Error while creating '" + outputDirectoryPath + "' directory");
                    System.out.println(Thread.currentThread().getName() + ": Error while creating '" + outputDirectoryPath + "' directory");
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + ": Error while creating '" + directory + "' directory");
            System.out.println(Thread.currentThread().getName() + ": Error while creating '" + directory + "' directory");
            System.out.println(e.getMessage());
        } finally {
            folderLock.unlock();
        }
    }

    public void deleteCheckpoints(String programId) {
        try {
            for (String file : createdFiles) {
                Path path = Paths.get(file);
                Files.deleteIfExists(path);
                logger.info(Thread.currentThread().getName() + ": Deleted checkpoint file " + file);
            }
            deleteEmptyDirectory(new File(CHECKPOINT_DIRECTORY + programId + "/"));
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + ": Error while deleting checkpoints");
            System.out.println(Thread.currentThread().getName() + ": Error while deleting checkpoints");
        }
    }

    private static void deleteEmptyDirectory(File folder) {
        try {
            folderLock.lock();
            if (folder.exists() && folder.isDirectory() && Objects.requireNonNull(folder.list()).length == 0) {
                logger.info(Thread.currentThread().getName() + ": Deleting '" + folder.getName() + "' directory");
                boolean result = folder.delete();
                if (result) {
                    logger.info(Thread.currentThread().getName() + ": Deleted '" + folder.getName() + "' directory");
                } else {
                    throw new Exception("Error while deleting '" + folder.getName() + "' directory");
                }
            }
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + ": Error while deleting '" + folder.getName() + "' directory");
            System.out.println(Thread.currentThread().getName() + ": Error while deleting '" + folder.getName() + "' directory");
        } finally {
            folderLock.unlock();
        }
    }

}
