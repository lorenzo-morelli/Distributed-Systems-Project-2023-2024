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

public class HadoopFileReadWrite {
    private static String HDFS_URI = "hdfs://localhost:9000";

    public synchronized static void setHDFS_URI(String newURI) {
        HDFS_URI = newURI;
    }

    private synchronized static void writeToHDFS(String content, String hdfsPath) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);

        Path outputPath = new Path(hdfsPath);
        FSDataOutputStream outputStream = fs.create(outputPath);

        // Write the content to the file on HDFS
        outputStream.write(content.getBytes()); // Write content as bytes

        outputStream.close();
        fs.close();
    }

    private synchronized static List<KeyValuePair> readFromHDFS(String hdfsPath) throws IOException {
        List<KeyValuePair> result = new ArrayList<>();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);

        try {
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));

            for (FileStatus fileStatus : fileStatuses) {
                result.addAll(readInputFile(fileStatus.getPath().toString()));
            }
        } finally {
            fs.close();
        }
        return result;
    }

    public synchronized static void writeKeys(String identifier,List<KeyValuePair> result) throws IOException {
    
        for (KeyValuePair pair : result) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            String fileName = "/key" + key +"/"+identifier+".csv"; 
    
            writeToHDFS(key + "," + value, fileName);                
            System.out.println("File created for key " + key + ": " + fileName);
        }
    }

    public static List<KeyValuePair> readKey(Integer key) throws IOException{
        List<KeyValuePair> result = new ArrayList<>(); 
        String fileName = "/key" + key;
        List<KeyValuePair> partialResult = readFromHDFS(fileName);
        result.addAll(partialResult);
        
        return result;
    }
    private static void uploadFileToHDFS(String localFilePath, String hdfsDestinationPath, Configuration conf) throws IOException {
        
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

        System.out.println("File "+finalName +" uploaded to HDFS successfully.");
    }
    
    public static void updloadFiles(List<String> list, String hdfsDestinationPath) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        
        for(String localFilePath : list){
            uploadFileToHDFS(localFilePath,hdfsDestinationPath, conf);
        }
    }
    public synchronized static List<KeyValuePair> readInputFile(String path) throws IOException{
        
        List<KeyValuePair> result = new ArrayList<>();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);


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
                    throw new IOException("Invalid line in CSV: " + line);
                }
            }
        } finally {
            fs.close();
        }

        return result;
    }
    public static void deleteFiles() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);

            fs.delete(new Path("/input"), true);
            
            FileStatus[] keyFileStatus = fs.globStatus(new Path("/key*"));
            if (keyFileStatus != null) {
                for (FileStatus fileStatus : keyFileStatus) {
                    fs.delete(fileStatus.getPath(), true);
                }
            }          
            System.out.println("Files deleted");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}