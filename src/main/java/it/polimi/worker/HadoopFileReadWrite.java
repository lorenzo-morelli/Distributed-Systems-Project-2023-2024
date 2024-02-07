package it.polimi.worker;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.K;

import it.polimi.common.KeyValuePair;

public class HadoopFileReadWrite {
    private static String HDFS_URI = "hdfs://localhost:9000";

    public static void setHDFS_URI(String newURI) {
        HDFS_URI = newURI;
    }

    private static void writeToHDFS(String content, String hdfsPath) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HDFS_URI);
            FileSystem fs = FileSystem.get(conf);

            Path outputPath = new Path(hdfsPath);
            FSDataOutputStream outputStream = fs.create(outputPath);

            // Write the content to the file on HDFS
            outputStream.writeUTF(content);

            outputStream.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<KeyValuePair> readFromHDFS(String hdfsPath) throws IOException {
        List<KeyValuePair> result = new ArrayList<>();

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URI);
        FileSystem fs = FileSystem.get(conf);

        try {
            Path filePath = new Path(hdfsPath);
            FileStatus[] fileStatuses = fs.listStatus(filePath);

            for (FileStatus fileStatus : fileStatuses) {
                FSDataInputStream inputStream = fs.open(fileStatus.getPath());

                try {
                    String content = inputStream.readUTF();

                    String[] parts = content.split(",");
                    if (parts.length == 2) {
                        Integer key = Integer.parseInt(parts[0].trim());
                        Integer value = Integer.parseInt(parts[1].trim());
                        result.add(new KeyValuePair(key, value));
                    } else {
                        System.out.println("Invalid line in CSV: " + content);
                    }
                } finally {
                    inputStream.close();
                }
            }
        } finally {
            fs.close();
        }

        return result;
    }

    public static void writeKeys(String identifier,List<KeyValuePair> result) {
    
        for (KeyValuePair pair : result) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            String fileName = "/key" + key +"/"+identifier+".csv"; 
    
            writeToHDFS(key + "," + value, fileName);                
            System.out.println("File created for key " + key + ": " + fileName);
        }
    }

    public static List<KeyValuePair> readKeys(List<Integer> keys) {
        List<KeyValuePair> result = new ArrayList<>(); 
        for (Integer key : keys) {
            String fileName = "/key" + key;
            try{
                List<KeyValuePair> partialResult = readFromHDFS(fileName);
                result.addAll(partialResult);
            }catch(Exception e){
                System.out.println(e.getMessage());
            }   
        }
        return result;
    }
}
