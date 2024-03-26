package it.polimi.coordinator;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;


import it.polimi.common.HadoopFileManager;
/**
 * The HadoopCoordinator class is responsible for managing the files on HDFS at coordinator-side. 
 * It extends the HadoopFileManager class.
 * It contains methods to delete files, upload files, get keys size, merge files locally and upload merged files.
 */

public class HadoopCoordinator extends HadoopFileManager {
    
    /**
     * HadoopCoordinator class constructor. 
     * @param address it is the address of the HDFS.
     * @throws IOException if it is not possible to connect to the HDFS.
     */
    public HadoopCoordinator(String address) throws IOException {
        super(address, 16384);
        logger = LogManager.getLogger("it.polimi.Coordinator");
    }
    /**
     * This method deletes the files from HDFS.
     * @param programId it is the id of the program.
     * @param phase2 it is a boolean that indicates if the phase 2 is present or not.
     */
    public void deleteFiles(String programId, Boolean phase2) {
        logger.info(Thread.currentThread().getName() + ": Deleting files from HDFS");
        try {

            fs.delete(new Path("/input" + programId), true);
            fs.delete(new Path("/output" + programId), true);

            if (phase2) {
                fs.delete(new Path("/program" + programId), true);
            }

            System.out.println(Thread.currentThread().getName() + ": Files deleted");
            logger.info(Thread.currentThread().getName() + ": Files deleted from HDFS");
        } catch (IOException e) {
            logger.error(e);
            System.out.println(Thread.currentThread().getName() + ": Error deleting files from HDFS : " + e.getMessage());
        }
    }
    /**
     * This method uploads the files to HDFS.
     * @param localFilePath it is the path of the file to upload.
     * @param hdfsDestinationPath it is the path of the destination in HDFS.
     * @throws IOException if it is not possible to upload the file.
     */
    private void uploadFileToHDFS(String localFilePath, String hdfsDestinationPath) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Uploading file to HDFS: " + localFilePath);
        String finalName = hdfsDestinationPath + new Path(localFilePath).getName();

        InputStream in = new BufferedInputStream(new FileInputStream(localFilePath));

        FSDataOutputStream out = fs.create(new Path(finalName));

        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;

        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }

        out.close();
        in.close();
        logger.info(Thread.currentThread().getName() + ": File " + finalName + " uploaded to HDFS successfully.");
        System.out.println(Thread.currentThread().getName() + ": File " + finalName + " uploaded to HDFS successfully.");
    }
    /**
     * This method uploads the files to HDFS.
     * @param list it is the list of files to upload.
     * @param hdfsDestinationPath it is the path of the destination in HDFS.
     * @throws IOException if it is not possible to upload the files.
     */
    public void uploadFiles(List<String> list, String hdfsDestinationPath) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Uploading files to HDFS");
        for (String localFilePath : list) {
            uploadFileToHDFS(localFilePath, hdfsDestinationPath);
        }
        logger.info(Thread.currentThread().getName() + ": Files uploaded to HDFS successfully.");
    }
    /**
     * This method gets the size of the keys.
     * @param programId it is the id of the program.
     * @return the size of the keys to be processed.
     * @throws IOException if it is not possible to get the size of the keys.
     */
    public int getKeysSize(String programId) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Getting keys size from HDFS");
        String path = "/program" + programId;
        if (!fs.exists(new Path(path))) {
            return 0;
        }
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        return fileStatuses.length;
    }
    /**
     * This method merges the files locally.
     * @param outputId it is the id of the output.
     * @param programId it is the id of the program.
     * @param isSecondPhase it is a boolean that indicates if the phase 2 is present or not.
     * @throws IOException if it is not possible to merge the files.
     */
    public synchronized void mergeFiles(String outputId, String programId, boolean isSecondPhase) throws IOException {
        
        String hdfsFilePath = "/output" + programId;
        String localMergedFilePath = "result-" + outputId + ".csv";

        if(!fs.exists(new Path(hdfsFilePath)))
        {
            //create an empty file localMergedFilePath
            new FileOutputStream(localMergedFilePath).close();
            logger.info("File "+hdfsFilePath+ " doesn't exist!");
            return;
        }
        if(isSecondPhase){
            logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
        
            
            FileStatus[] foldersStatus = fs.listStatus(new Path(hdfsFilePath));
            for (FileStatus folderStatus : foldersStatus) {
                try (BufferedOutputStream mergedOut = new BufferedOutputStream(new FileOutputStream(localMergedFilePath, true))) {
                    FileStatus[] fileStatuses = fs.listStatus(new Path(folderStatus.getPath().toString()));

                    for (FileStatus fileStatus : fileStatuses) {
                        String fileName = fileStatus.getPath().toString();
                        downloadAndAppendToMergedFile(fileName, mergedOut);
                    }
                }
            }
        }else{
            logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
            try (BufferedOutputStream mergedOut = new BufferedOutputStream(new FileOutputStream(localMergedFilePath, true))) {
                
                FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsFilePath));

                for (FileStatus fileStatus : fileStatuses) {
                    String fileName = fileStatus.getPath().getName();
                    String hdfsFile = hdfsFilePath + "/" + fileName;
                    downloadAndAppendToMergedFile(hdfsFile, mergedOut);
                }
            }
        }
    }
    /**
     * This method downloads and appends the file to the final csv.
     * @param hdfsFilePath it is the path of the file in HDFS.
     * @param mergedOut it is the output stream of the merged file.
     * @throws IOException if it is not possible to download and append the file.
     */
    private void downloadAndAppendToMergedFile(String hdfsFilePath, OutputStream mergedOut){
        logger.info(Thread.currentThread().getName() + ": Downloading file from HDFS: " + hdfsFilePath);
        try (InputStream in = fs.open(new Path(hdfsFilePath))) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;

            while ((bytesRead = in.read(buffer)) > 0) {
                mergedOut.write(buffer, 0, bytesRead);
            }
        }catch( IOException e){
            logger.info("File"+hdfsFilePath+ "doesn't exist!");
        }
        logger.info(Thread.currentThread().getName() + ": File " + hdfsFilePath + " downloaded and appended to merged file successfully.");
    }
    /**
     * This method uploads the merged files to HDFS.
     * @param id it is the id of the program.
     * @param i it is the number of the task.
     * @param files it is the list of files to upload.
     * @throws IOException if it is not possible to upload the merged files.
     */
    public synchronized void uploadMergedFiles(String id, String i, List<String> files) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
        files.forEach(file -> logger.info(Thread.currentThread().getName() + ": File: " + file));
        String hdfsFilePath = "/input" + id;
        String localMergedFilePath = "task" + i + ".csv";

        FSDataOutputStream mergedOut = fs.create(new Path(hdfsFilePath + "/" + localMergedFilePath));
        for (String file : files) {
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
                byte[] buffer = new byte[BUFFER_SIZE];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) > 0) {
                    mergedOut.write(buffer, 0, bytesRead);
                }
            }
        }
        logger.info(Thread.currentThread().getName() + ": Files merged from HDFS successfully : " + hdfsFilePath + "/" + localMergedFilePath);
        mergedOut.close();
    }


}
