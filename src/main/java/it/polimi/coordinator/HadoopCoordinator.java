package it.polimi.coordinator;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;

import it.polimi.common.HadoopFileManager;

public class HadoopCoordinator extends HadoopFileManager{
    public HadoopCoordinator(String address) throws IOException{
        super(address);
        logger = LogManager.getLogger("it.polimi.Coordinator");
    }

    public void deleteFiles(String programId) {
        logger.info("Deleting files from HDFS");
        try {

            fs.delete(new Path("/input" + programId), true);
            fs.delete(new Path("/program" + programId), true);
    
            System.out.println("Files deleted");
            logger.info("Files deleted from HDFS");
        } catch (IOException e) {
            logger.error(e);
            e.printStackTrace();
        }
        logger.info("Closing file system");
        closeFileSystem();
        logger.info("File system closed");
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
}
