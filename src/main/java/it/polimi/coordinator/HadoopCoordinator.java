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
        super(address,16384);
        logger = LogManager.getLogger("it.polimi.Coordinator");
    }

    public void deleteFiles(String programId, Boolean phase2) {
        logger.info(Thread.currentThread().getName() + ": Deleting files from HDFS");
        try {
            
            fs.delete(new Path("/input" + programId), true);
            fs.delete(new Path("/output" + programId), true);

            if(phase2){
                fs.delete(new Path("/program" + programId), true);
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

        InputStream in = new BufferedInputStream(new FileInputStream(localFilePath)); 

        FSDataOutputStream out = fs.create(new Path(finalName));

        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        
        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, 0, bytesRead);
        }

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
        logger.info(Thread.currentThread().getName() + ": Getting keys size from HDFS");
        String path = "/program"+programId;
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        return fileStatuses.length;
    } 

    public synchronized void mergeFiles(String outputId,String programId, int identifier) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
        String hdfsFilePath = "/output" + programId + "/" + identifier;
        String localMergedFilePath = "result-" + outputId + ".csv";
    
        try (BufferedOutputStream mergedOut = new BufferedOutputStream(new FileOutputStream(localMergedFilePath, true))) {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsFilePath));
    
            for (FileStatus fileStatus : fileStatuses) {
                String fileName = fileStatus.getPath().getName();
                String hdfsFile = hdfsFilePath + "/" + fileName;
                downloadAndAppendToMergedFile(hdfsFile, mergedOut);
            }
        }
    }
    public void mergeFiles(String outputId,String programId) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
        String hdfsFilePath = "/output" + programId;
        String localMergedFilePath = "result-" + outputId + ".csv";
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
        try (InputStream in = fs.open(new Path(hdfsFilePath))) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
    
            while ((bytesRead = in.read(buffer)) > 0) {
                mergedOut.write(buffer, 0, bytesRead);
            }
        }
        logger.info(Thread.currentThread().getName() + ": File " + hdfsFilePath + " downloaded and appended to merged file successfully.");
    }

    public synchronized void uploadMergedFiles(String id,String i, List<String> files) throws IOException{
        logger.info(Thread.currentThread().getName() + ": Merging files from HDFS");
        files.stream().forEach(file -> logger.info(Thread.currentThread().getName() + ": File: " + file));
        String hdfsFilePath = "/input" + id;
        String localMergedFilePath = "task" + i + ".csv";

        FSDataOutputStream mergedOut = fs.create(new Path(hdfsFilePath + "/" + localMergedFilePath));
        for (String file : files) {
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file)) ) {
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
