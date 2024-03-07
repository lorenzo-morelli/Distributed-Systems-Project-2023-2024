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

public class HadoopCoordinator extends HadoopFileManager{
    public HadoopCoordinator(String address) throws IOException{
        super(address);
        logger = LogManager.getLogger("it.polimi.Coordinator");
    }

    public void deleteFiles(String programId, Boolean phase2) {
        logger.info(Thread.currentThread().getName() + ": Deleting files from HDFS");
        try {
            
            fs.delete(new Path("/input" + programId), true);
            fs.delete(new Path("/program" + programId), true);
            
            if(phase2){
                fs.delete(new Path("/output" + programId), true);
            }

            System.out.println(Thread.currentThread().getName() + ": Files deleted");
            logger.info(Thread.currentThread().getName() + ": Files deleted from HDFS");
        } catch (IOException e) {
            logger.error(e);
            System.out.println(Thread.currentThread().getName() + ": Error deleting files from HDFS : " + e.getMessage());
        }
    }
    private void uploadFileToHDFS(String localFilePath, String hdfsDestinationPath) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Uploading file to HDFS: " + localFilePath);
        String finalName =  hdfsDestinationPath + new Path(localFilePath).getName();

        // Open the local file
        InputStream in = new BufferedInputStream(new FileInputStream(localFilePath)); 

        // Create HDFS output stream
        FSDataOutputStream out = fs.create(new Path(finalName));

        // Set buffer size to 4KB
        byte[] buffer = new byte[4096];
        int bytesRead;
        
        // Read file in chunks and write to HDFS
        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }

        // Close the streams
        out.close();
        in.close();
        logger.info(Thread.currentThread().getName() + ": File "+finalName +" uploaded to HDFS successfully.");
        System.out.println(Thread.currentThread().getName() +": File "+finalName +" uploaded to HDFS successfully.");
    }
    public void uploadFiles(List<String> list, String hdfsDestinationPath) throws IOException{
        logger.info(Thread.currentThread().getName() +": Uploading files to HDFS");
        for(String localFilePath : list){
            uploadFileToHDFS(localFilePath,hdfsDestinationPath);
        }
        logger.info(Thread.currentThread().getName() +": Files uploaded to HDFS successfully.");
    }
    
    public int getKeysSize(String programId) throws IOException {
        String path = "/program"+programId;
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        return fileStatuses.length;
    } 

    public synchronized void mergeFiles(String programId, int identifier) throws IllegalArgumentException, IOException {
        String hdfsFilePath = "/output" + programId + "/" + identifier;
        String localMergedFilePath = "result-" + programId + ".csv";
    
        // Open the output file in append mode
        try (BufferedOutputStream mergedOut = new BufferedOutputStream(new FileOutputStream(localMergedFilePath, true))) {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsFilePath));
    
            for (FileStatus fileStatus : fileStatuses) {
                String fileName = fileStatus.getPath().getName();
                String hdfsFile = hdfsFilePath + "/" + fileName;
                downloadAndAppendToMergedFile(hdfsFile, mergedOut);
            }
        }
    }
    public void mergeFiles(String programId) throws IllegalArgumentException, IOException {
        String hdfsFilePath = "/program" + programId;
        String localMergedFilePath = "result-" + programId + ".csv";
        // Open the output file in append mode
        try (BufferedOutputStream mergedOut = new BufferedOutputStream(new FileOutputStream(localMergedFilePath, true))) {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsFilePath));
    
            for (FileStatus fileStatus : fileStatuses) {
                String fileName = fileStatus.getPath().getName();
                String hdfsFile = hdfsFilePath + "/" + fileName;
                downloadAndAppendToMergedFile(hdfsFile, mergedOut);
            }
        }
    }

    private void downloadAndAppendToMergedFile(String hdfsFilePath, OutputStream mergedOut) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Downloading file from HDFS: " + hdfsFilePath);
        // Open HDFS file
        try (InputStream in = fs.open(new Path(hdfsFilePath))) {
            // Set buffer size to 4KB
            byte[] buffer = new byte[4096];
            int bytesRead;
    
            // Read file in chunks and write to merged file
            while ((bytesRead = in.read(buffer)) > 0) {
                mergedOut.write(buffer, 0, bytesRead);
            }
        }
        logger.info(Thread.currentThread().getName() + ": File " + hdfsFilePath + " downloaded and appended to merged file successfully.");
    }

    public synchronized void mergeHadoopFiles(String id,String i, List<String> files) {
        //merge the local files into 1 in hdfs in the folder /program+programId
        String hdfsFilePath = "/input" + id;
        String localMergedFilePath = "program" + i + ".csv";

        try (FSDataOutputStream mergedOut = fs.create(new Path(hdfsFilePath + "/" + localMergedFilePath))) {
            for (String file : files) {
                try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file)) ) {
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        mergedOut.write(buffer, 0, bytesRead);
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e);
            System.out.println(Thread.currentThread().getName() + ": Error merging files: " + e.getMessage());
        }
    }
    


}
