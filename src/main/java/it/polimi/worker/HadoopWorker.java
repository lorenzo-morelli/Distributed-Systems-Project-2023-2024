package it.polimi.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;

import java.util.regex.Matcher;
import it.polimi.common.HadoopFileManager;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.common.messages.NormalOperations;

public class HadoopWorker extends HadoopFileManager{
    private static final int BUFFER_SIZE = 8192;  // 8KB buffer size, adjust as needed

    public HadoopWorker(String address) throws IOException{
        super(address);
        logger = LogManager.getLogger("it.polimi.Worker");
    }

    public void readInputFile(int i, NormalOperations task, WorkerHandler workerHandler) throws IOException {
        
        Path filePath = new Path(task.getPathFiles().get(i));
        // Open the HDFS input stream
        FSDataInputStream in = fs.open(filePath);
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        List<KeyValuePair> result = new ArrayList<>();
        StringBuilder currentLine = new StringBuilder();
        Integer count = 0;
        while ((bytesRead = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
            currentLine.append(new String(buffer, 0, bytesRead));

            int newlineIndex;
            while ((newlineIndex = currentLine.indexOf("\n")) != -1) {
                String line = currentLine.substring(0, newlineIndex).trim();
                processLine(line, result);  
                currentLine.delete(0, newlineIndex + 1);
            }
            workerHandler.processPartitionTask(result, task,i,count);
            result.clear();
            count++;
        }
    }
    private void processLine(String line, List<KeyValuePair> result) throws IOException {
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
    private String getPath(String programId, String identifier,Integer currentKey, boolean phase2){
        return phase2 ? "/output"+programId+"/"+identifier +"/key"+currentKey+".csv"
                : "/program"+programId+"/key" + currentKey +"/"+identifier+".csv";
    }

    public List<String> writeKeys(String programId, String identifier,List<KeyValuePair> result, boolean phase2) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        List<String> filesWritten = new ArrayList<>();
        result.sort((KeyValuePair p1, KeyValuePair p2) -> p1.getKey().compareTo(p2.getKey()));
        if(result.isEmpty()){
            return filesWritten;
        }
        Integer count = 0;
        Integer currentKey = result.get(count).getKey();
        Integer start = 0;
        Integer end = 0;
        
        for(;count<result.size();count++){
            Integer key = result.get(count).getKey();    
            if(currentKey!= key){
                String fileName = getPath(programId, identifier, currentKey, phase2);
                currentKey = key;
                writeResult(result.subList(start, end),fileName);
                filesWritten.add(fileName);             
                start = end;
                end++;
            }else{
                end++;
            }
        }
        String fileName = getPath(programId, identifier, currentKey, phase2);
        writeResult(result.subList(start, end),fileName);
        filesWritten.add(fileName);
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
        return filesWritten;
    } 

    private void writeResult(List<KeyValuePair> data,String path) throws IOException {
        FileSystem fileSystem = initialize();

        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + path);
        Path outputPath = new Path(path);
        FSDataOutputStream outputStream =  fileSystem.create(outputPath);
        for (KeyValuePair pair : data) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            outputStream.write((key + "," + value + "\n").getBytes());
        }
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + path);
        fileSystem.close();
        outputStream.close();
    }   

  
    public List<KeyValuePair> readAndComputeReduce(int idx, ReduceOperation reduceMessage, Operator reduce) throws IOException, IllegalArgumentException {
        
        Integer key = findKey(idx, reduceMessage.getProgramId());

        String path = "/program"+reduceMessage.getProgramId()+"/key" + key; 
        List<KeyValuePair> result = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            FSDataInputStream in = fs.open(filePath);
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            StringBuilder currentLine = new StringBuilder();

            while ((bytesRead = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
                currentLine.append(new String(buffer, 0, bytesRead));

                int newlineIndex;
                while ((newlineIndex = currentLine.indexOf("\n")) != -1) {
                    String line = currentLine.substring(0, newlineIndex).trim();
                    processLine(line, result);
                    currentLine.delete(0, newlineIndex + 1);

                }
                result = reduce.execute(result);
            }
        }
        return result;
    }
    private Integer findKey(int idx, String programId) throws IOException{
        String path = "/program"+programId;
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        Pattern pattern = Pattern.compile("key(\\d+)");
        Matcher matcher = pattern.matcher(fileStatuses[idx].getPath().getName());
        
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new IllegalArgumentException("Invalid Key file");
        }
    }
}
