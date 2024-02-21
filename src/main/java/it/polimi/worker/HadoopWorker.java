package it.polimi.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;

import it.polimi.common.HadoopFileManager;
import it.polimi.common.KeyValuePair;

public class HadoopWorker extends HadoopFileManager{
    
    public HadoopWorker(String address) throws IOException{
        super(address);
        logger = LogManager.getLogger("it.polimi.Worker");
    }

    private void writeToHDFS(String content, String hdfsPath) throws IOException{
        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + hdfsPath);
       
        Path outputPath = new Path(hdfsPath);
        FSDataOutputStream outputStream = fs.create(outputPath);

        // Write the content to the file on HDFS
        outputStream.write(content.getBytes()); // Write content as bytes

        outputStream.close();
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + hdfsPath);
    }

    private List<KeyValuePair> readFromHDFS(String hdfsPath) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Reading from HDFS: " + hdfsPath);
        List<KeyValuePair> result = new ArrayList<>();

        

        FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));

        for (FileStatus fileStatus : fileStatuses) {
            result.addAll(readInputFile(fileStatus.getPath().toString()));
        }
      
        logger.info(Thread.currentThread().getName() +": Read from HDFS: " + hdfsPath);
        return result;
    }

    public void writeKeys(String programId, String identifier,List<KeyValuePair> result) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        for (KeyValuePair pair : result) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            String fileName = "/program"+programId+"/key" + key +"/"+identifier+".csv"; 
    
            writeToHDFS(key + "," + value, fileName);                
        }
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
    }

    public List<KeyValuePair> readKey(String programId, Integer key) throws IOException{
        logger.info(Thread.currentThread().getName() + ": Reading key " + key + " from HDFS");
        List<KeyValuePair> result = new ArrayList<>(); 
        String fileName = "/program"+ programId + "/key" + key;
        List<KeyValuePair> partialResult = readFromHDFS(fileName);
        result.addAll(partialResult);
        logger.info(Thread.currentThread().getName() + ": Key " + key + " read from HDFS");
        return result;
    }
    
    
    
    public List<KeyValuePair> readInputFile(String path) throws IOException{
        logger.info(Thread.currentThread().getName() + ": Reading input file from HDFS: " + path);

        List<KeyValuePair> result = new ArrayList<>();
       

        Path filePath = new Path(path);

        // Open the HDFS input stream
        FSDataInputStream in = fs.open(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

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
        
        logger.info(Thread.currentThread().getName() + ": Input file read from HDFS: " + path);
        return result;
    }
}
