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
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.log4j.LogManager;

import java.util.regex.Matcher;
import it.polimi.common.HadoopFileManager;
import it.polimi.common.KeyValuePair;
import it.polimi.common.messages.ReduceOperation;
import it.polimi.worker.operators.ReduceOperator;
import it.polimi.worker.utils.Data;
import it.polimi.common.messages.NormalOperations;

public class HadoopWorker extends HadoopFileManager{

    public HadoopWorker(String address) throws IOException{
        super(address,67108864);
        logger = LogManager.getLogger("it.polimi.Worker");
    }

    public void readInputFile(int i, NormalOperations task, WorkerHandler workerHandler, List<Operator> operators, int count) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Reading input file");
        Path filePath = new Path(task.getPathFiles().get(i));
        // Open the HDFS input stream
        FSDataInputStream in = fs.open(filePath);

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
        Data data;
        
        while ((data = readFile(in, count)).getData()!="" || partialTuple.toString().length() > 0){
            logger.info(Thread.currentThread().getName() + ": Data is ready to be processed of partition: " + count + " of file: " + i);
            String combinedData = data.getData() == "" ? partialTuple.toString() : partialTuple.toString() + data.getData();
           
            String[] lines = combinedData.split("\n");
            if(combinedData.endsWith("\n")){
                for (int j = 0; j < lines.length; j++) {
                    processLine(lines[j].trim(), result);
                }
                partialTuple = new StringBuilder();
            }
            else{
                for (int j = 0; j < lines.length - 1; j++) {
                    processLine(lines[j].trim(), result);
                }
                partialTuple = new StringBuilder(lines[lines.length - 1]);
            }

            for (Operator op : operators) {
                result = op.execute(result);
            }
            if ((task.getReduce() && !task.getChangeKey())) {
                if (reduceResult == null) {
                    reduceResult = result.get(0);
                } else {
                    result.add(reduceResult);
                    reduceResult = reduce.execute(result).get(0);
                }
            } else {
                workerHandler.processPartitionTask(result, task, i, count, data.getEnd() && partialTuple.toString().length() == 0);
            }
            logger.info(Thread.currentThread().getName() + ": Data processed of partition: " + count + " of file: " + i);
            result.clear();
            count++;
        }
        if(task.getReduce() && !task.getChangeKey()){
            workerHandler.processPartitionTask(List.of(reduceResult), task,i,0, true);
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
        logger.info(Thread.currentThread().getName() + ": Reading and computing reduce for key: " + key);
        System.out.println(Thread.currentThread().getName() + ": Reading and computing reduce for key: " + key);
        String path = "/program"+reduceMessage.getProgramId()+"/key" + key; 
        List<KeyValuePair> result = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            FSDataInputStream in = fs.open(filePath);
           
            
            StringBuilder partialTuple  = new StringBuilder();
            Integer count = 0;
            Data data;
           
            while ((data = readFile(in, count)).getData()!="" || partialTuple.toString().length() > 0){
                
                System.out.println(Thread.currentThread().getName() + ": Data is ready to be processed of partition: " + count + " of key: " + key);

                String combinedData = data.getData() == "" ? partialTuple.toString() : partialTuple.toString() + data.getData();
                
               
                String[] lines = combinedData.split("\n");
                if(combinedData.endsWith("\n")){
                    for (int j = 0; j < lines.length; j++) {
                        processLine(lines[j].trim(), result);
                    }
                    partialTuple = new StringBuilder();
                }
                else{
                    for (int j = 0; j < lines.length - 1; j++) {
                        processLine(lines[j].trim(), result);
                    }
                    partialTuple = new StringBuilder(lines[lines.length - 1]);
                }
                count++;
                result = reduce.execute(result);
            }
        }
        logger.info(Thread.currentThread().getName() + ": Reduce has been computed for key: " + key);
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
    private Data readFile(FSDataInputStream in, int numPart) throws IOException{
        
        String data = "";
        
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead = 0;
        long accumulatedBytesRead = 0;
        DFSInputStream dfsInputStream = (DFSInputStream) in.getWrappedStream();
        long fileLength = dfsInputStream.getFileLength();
        long seekPosition = (long) numPart * BUFFER_SIZE;
        

        if (seekPosition >= fileLength) {
            return new Data("", true);  // End of file
        }
        in.seek(seekPosition);
        while (accumulatedBytesRead  < BUFFER_SIZE) {
            if ((bytesRead = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
                // Convert only the read portion of the buffer to a string
                data += new String(buffer, 0, bytesRead);
                accumulatedBytesRead += bytesRead;
            }
            else {
                break;
            }
        }
        logger.info(Thread.currentThread().getName() + ": Read " + accumulatedBytesRead + " bytes from file");
        return new Data(data, seekPosition + accumulatedBytesRead >= fileLength);
    }
}
