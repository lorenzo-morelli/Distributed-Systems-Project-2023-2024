package it.polimi.common;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class HadoopFileReadWrite {
    private static String HDFS_URI = "hdfs://localhost:9000";
    
    private static Logger logger;

    public synchronized static void setHDFS_URI(String newURI) {
        HDFS_URI = newURI;
    }

    private synchronized static FileSystem initialize() throws IOException{
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
		return fs;
	}


    private synchronized static void writeToHDFS(String content, String hdfsPath) throws IOException{
        logger = LogManager.getLogger("it.polimi.Worker");
        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + hdfsPath);
       
        FileSystem fs = initialize();

        Path outputPath = new Path(hdfsPath);
        FSDataOutputStream outputStream = fs.create(outputPath);

        // Write the content to the file on HDFS
        outputStream.write(content.getBytes()); // Write content as bytes

        outputStream.close();
        fs.close();
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + hdfsPath);
    }

    private synchronized static List<KeyValuePair> readFromHDFS(String hdfsPath) throws IOException {
        logger = LogManager.getLogger("it.polimi.Worker");
        logger.info(Thread.currentThread().getName() + ": Reading from HDFS: " + hdfsPath);
        List<KeyValuePair> result = new ArrayList<>();

        
        FileSystem fs = initialize();

        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));

            for (FileStatus fileStatus : fileStatuses) {
                result.addAll(readInputFile(fileStatus.getPath().toString()));
            }
        } finally {
            fs.close();
        }
        logger.info(Thread.currentThread().getName() +": Read from HDFS: " + hdfsPath);
        return result;
    }

    public synchronized static void writeKeys(Integer id, String identifier,List<KeyValuePair> result) throws IOException {
        logger = LogManager.getLogger("it.polimi.Worker");
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        for (KeyValuePair pair : result) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            String fileName = "/"+id+"key" + key +"/"+identifier+".csv"; 
    
            writeToHDFS(key + "," + value, fileName);                
        }
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
    }

    public static List<KeyValuePair> readKey(Integer id, Integer key) throws IOException{
        logger = LogManager.getLogger("it.polimi.Worker");
        logger.info(Thread.currentThread().getName() + ": Reading key " + key + " from HDFS");
        List<KeyValuePair> result = new ArrayList<>(); 
        String fileName = "/"+ id + "key" + key;
        List<KeyValuePair> partialResult = readFromHDFS(fileName);
        result.addAll(partialResult);
        logger.info(Thread.currentThread().getName() + ": Key " + key + " read from HDFS");
        return result;
    }
    private synchronized static void uploadFileToHDFS(String localFilePath, String hdfsDestinationPath, Configuration conf) throws IOException {
        logger = LogManager.getLogger("it.polimi.Coordinator");
        logger.info("Uploading file to HDFS: " + localFilePath);
        String finalName =  hdfsDestinationPath + new Path(localFilePath).getName();
        // Get the Hadoop FileSystem object
        FileSystem fs = FileSystem.get(conf);

        // Open the local file
        try (InputStream in = new BufferedInputStream(new FileInputStream(localFilePath))) {

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
        } finally {
            // Close the FileSystem object
            fs.close();
        }
        logger.info("File "+finalName +" uploaded to HDFS successfully.");
        System.out.println("File "+finalName +" uploaded to HDFS successfully.");
    }
    
    public static void uploadFiles(List<String> list, String hdfsDestinationPath) throws IOException{
        logger = LogManager.getLogger("it.polimi.Coordinator");
        logger.info("Uploading files to HDFS");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        for(String localFilePath : list){
            uploadFileToHDFS(localFilePath,hdfsDestinationPath, conf);
        }
        logger.info("Files uploaded to HDFS successfully.");
    }
    public synchronized static List<KeyValuePair> readInputFile(String path) throws IOException{
        logger = LogManager.getLogger("it.polimi.Worker");
        logger.info(Thread.currentThread().getName() + ": Reading input file from HDFS: " + path);

        List<KeyValuePair> result = new ArrayList<>();
       
        FileSystem fs = initialize();


        Path filePath = new Path(path);

        // Open the HDFS input stream
        try (FSDataInputStream in = fs.open(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    Integer key = Integer.parseInt(parts[0].trim());
                    Integer value = Integer.parseInt(parts[1].trim());
                    result.add(new KeyValuePair(key, value));
                } else {
                    System.out.println("Invalid line in CSV: " + line);
                    logger.error(Thread.currentThread().getName() + ": Invalid line in CSV: " + line);
                    throw new IOException("Invalid line in CSV: " + line);
                }
            }
        } finally {
            fs.close();
        }
        logger.info(Thread.currentThread().getName() + ": Input file read from HDFS: " + path);
        return result;
    }
    public synchronized static void deleteFiles(Integer id) {
        logger = LogManager.getLogger("it.polimi.Coordinator");
        logger.info("Deleting files from HDFS");
     
        FileSystem fs = null;
        try {
            fs =initialize();

            fs.delete(new Path("/input" + id), true);
            
            FileStatus[] keyFileStatus = fs.globStatus(new Path("/"+ id + "*"));
            if (keyFileStatus != null) {
                for (FileStatus fileStatus : keyFileStatus) {
                    fs.delete(fileStatus.getPath(), true);
                }
            }          
            System.out.println("Files deleted");
            logger.info("Files deleted from HDFS");
        } catch (IOException e) {
            logger.error(e);
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                logger.error(e);
                e.printStackTrace();
            }
        }
    }
}