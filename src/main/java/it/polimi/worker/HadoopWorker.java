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
import it.polimi.worker.operators.ReduceOperator;
import it.polimi.common.messages.NormalOperations;

public class HadoopWorker extends HadoopFileManager{
    private int BUFFER_SIZE = 262144;  // 64KB buffer size, adjust as needed

    public HadoopWorker(String address) throws IOException{
        super(address);
        logger = LogManager.getLogger("it.polimi.Worker");
    }

    public void readInputFile(int i, NormalOperations task, WorkerHandler workerHandler, List<Operator> operators) throws IOException {
        
        Path filePath = new Path(task.getPathFiles().get(i));
        // Open the HDFS input stream
        FSDataInputStream in = fs.open(filePath);

        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        List<KeyValuePair> result = new ArrayList<>();

        Operator reduce = null;
        KeyValuePair reduceResult = null;
        if(task.getReduce() && !task.getChangeKey()){
            for (Operator op : operators) {
                if (op instanceof ReduceOperator) {
                    reduce = op;
                    break;
                }
            }
        }

        StringBuilder partialTuple  = new StringBuilder();
        Integer count = 0;
        while ((bytesRead = in.read(buffer)) > 0) {
            String data = new String(buffer, 0, bytesRead);
            String combinedData = partialTuple.toString() + data;

            String[] lines = combinedData.split("\n");

            for (int j = 0; j < lines.length - 1; j++) {
                processLine(lines[j].trim(), result);
            }
            partialTuple = new StringBuilder(lines[lines.length - 1]);

            if((task.getReduce() && !task.getChangeKey())){
                if(reduceResult == null){
                    reduceResult = result.get(0);
                }else{
                    result.add(reduceResult);
                    reduceResult = reduce.execute(result).get(0);
                }
            }else{
                workerHandler.processPartitionTask(result, task,i,count);
            }
            result.clear();
            count++;
        }
        if(task.getReduce() && !task.getChangeKey()){
            workerHandler.processPartitionTask(List.of(reduceResult), task,i,count);
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
    

    public void writeKeys(String programId, String identifier,KeyValuePair result) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        String fileName = "/output"+programId+"/"+identifier +"/key"+result.getKey()+".csv";
        FileSystem fileSystem = initialize();
        writeResult(List.of(result),fileName,fileSystem);
        fileSystem.close();
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
    } 

    public void writeKeys(String programId, String identifier,List<KeyValuePair> result,boolean changeKey,boolean reduce) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        if(result.isEmpty()){
            return;
        }
        FileSystem fileSystem = initialize();

        if(changeKey && reduce){
            for(KeyValuePair pair: result){
                String fileName = "/program"+programId+"/key" + pair.getKey() +"/"+identifier+".csv";
                writeResult(List.of(pair), fileName,fileSystem);
            }   
        }
        else{
            String fileName = "/program"+programId+"/" + identifier + ".csv";
            writeResult(result,fileName,fileSystem);
        }
        fileSystem.close();

    } 

    private void writeResult(List<KeyValuePair> data,String path, FileSystem fileSystem) throws IOException {

        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + path);
        Path outputPath = new Path(path);
        FSDataOutputStream outputStream =  fileSystem.create(outputPath);
        for (KeyValuePair pair : data) {
            Integer key = pair.getKey();
            Integer value = pair.getValue();
            outputStream.write((key + "," + value + "\n").getBytes());
        }
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + path);
        outputStream.close();
    }   

  
    public KeyValuePair readAndComputeReduce(int idx, ReduceOperation reduceMessage, Operator reduce) throws IOException, IllegalArgumentException {
        
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

            while ((bytesRead = in.read(buffer)) > 0) {
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
        return result.get(0);
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
