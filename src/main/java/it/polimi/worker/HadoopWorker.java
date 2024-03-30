package it.polimi.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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
import it.polimi.worker.models.Data;
import it.polimi.worker.models.Operator;
import it.polimi.common.messages.NormalOperations;

/**
 * This class is responsible for reading and writing data from HDFS at worker-side.
 * It also processes the data and executes the operators on the data.
 */
public class HadoopWorker extends HadoopFileManager {
    /**
     * HadoopWorker class constructor
     * @param address it is the address of the HDFS.
     * @throws IOException if it is not possible to connect to the HDFS.
     */
    public HadoopWorker(String address) throws IOException {
        super(address, 131072);
        logger = LogManager.getLogger("it.polimi.Worker");
    }
    /**
     * The processData method processes the data and executes the operators on the data.
     * It splits the data into lines and processes each line.
     * @param combinedData it is the data to be processed.
     * @param result it is the list of KeyValuePair where the data will be stored.
     * @return the partial tuple.
     * @throws IOException if there is an error processing the data.
     */
    public StringBuilder processData(String combinedData, List<KeyValuePair> result) throws IOException {
        StringBuilder partialTuple;
        String[] lines = combinedData.split("\n");

        if (combinedData.endsWith("\n")) {
            for (String line : lines) {
                processLine(line.trim(), result);
            }
            partialTuple = new StringBuilder();
        } else {
            for (int j = 0; j < lines.length - 1; j++) {
                processLine(lines[j].trim(), result);
            }
            partialTuple = new StringBuilder(lines[lines.length - 1]);
        }
        return partialTuple;

    }
    /**
     * The readInputFile method reads the input file and processes the data.
     * It reads the data from the input file and processes it using the operators.
     * It also calls the WorkerHandler to write the processed data to HDFS.
     * @param i it is the index of the file.
     * @param task it is the NormalOperations object.
     * @param workerHandler it is the WorkerHandler object.
     * @param operators it is the list of operators.
     * @param count it is the partition count, used to identify the partition.
     * @param remainingString it is the remaining string read from the checkpoint, if any.
     * @param value it is the KeyValuePair object read from the checkpoint, if any.
     * @throws IOException if there is an error reading the input file.
     */
    public void readInputFile(int i, NormalOperations task, WorkerHandler workerHandler, List<Operator> operators, int count, String remainingString, KeyValuePair value) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Reading input file: " + task.getPathFiles().get(i));
        Path filePath = new Path(task.getPathFiles().get(i));
        FSDataInputStream in = fs.open(filePath);

        List<KeyValuePair> result = new ArrayList<>();

        Operator reduce = null;
        KeyValuePair reduceResult = value;
        if (task.getReduce() && !task.getChangeKey()) {
            for (Operator op : operators) {
                if (op instanceof ReduceOperator) {
                    reduce = op;
                    break;
                }
            }
        }

        StringBuilder partialTuple = new StringBuilder(remainingString);
        Data data;

        while ((!(data = readFile(in, count)).data().isEmpty()) || !partialTuple.toString().isEmpty()) {
            logger.info(Thread.currentThread().getName() + ": Data is ready to be processed of partition: " + count + " of file: " + task.getPathFiles().get(i));

            String combinedData = data.data().isEmpty() ? partialTuple.toString() : partialTuple + data.data();

            partialTuple = processData(combinedData, result);

            count++;
            for (Operator op : operators) {
                result = op.execute(result);
            }
            if(!(result==null || result.isEmpty())){
            if ((task.getReduce() && !task.getChangeKey())) {
                if (reduceResult == null) {
                    reduceResult = result.getFirst();
                } else {
                    result.add(reduceResult);
                    assert reduce != null;
                    reduceResult = reduce.execute(result).getFirst();
                }
                workerHandler.processPartitionTask(List.of(reduceResult), task, i, count, data.end() && partialTuple.toString().isEmpty(), partialTuple.toString(), data.end() && partialTuple.toString().isEmpty());
            } else {
                workerHandler.processPartitionTask(result, task, i, count, data.end() && partialTuple.toString().isEmpty(), partialTuple.toString(), true);

            }
            result.clear();
        }
            logger.info(Thread.currentThread().getName() + ": Data processed of partition: " + String.valueOf(count-1) + " of file: " + task.getPathFiles().get(i));
            
        }
        in.close();
    }
    /**
     * The processLine method processes the line of the data.
     * It splits the line into key and value and stores it in the result list.
     * @param line it is the line to be processed.
     * @param result it is the list of KeyValuePair where the data will be stored.
     * @throws IOException if there is an error processing the line.
     */
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
    /**
     * The writeKeys method writes the keys to HDFS.
     * It writes the keys to HDFS in the specified path.
     * This method is called in the reduce phase, i.e., the second phase of the program.
     * @param programId it is the id of the program.
     * @param identifier it is the identifier of the program.
     * @param result it is the KeyValuePair object to be written.
     * @throws IOException if there is an error writing the keys.
     */
    public void writeKeys(String programId, String identifier, KeyValuePair result) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        String fileName = "/output" + programId + "/" + identifier + "/key" + result.key() + ".csv";
        writeResult(List.of(result), fileName);
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
    }
    /**
     * The writeKeys method writes the keys to HDFS.
     * It writes the keys to HDFS in the specified path.
     * This method is called in the first phase of the program.
     * @param programId it is the id of the program.
     * @param identifier it is the identifier of the program.
     * @param result it is the list of KeyValuePair objects to be written.
     * @throws IOException if there is an error writing the keys.
     */
    public void writeKeys(String programId, String identifier, List<KeyValuePair> result, boolean changeKey, boolean reduce) throws IOException {
        if (result.isEmpty()) {
            return;
        }

        if (changeKey && reduce) {
            for (KeyValuePair pair : result) {
                String fileName = "/program" + programId + "/key" + pair.key() + "/" + identifier + ".csv";
                writeResult(List.of(pair), fileName);
            }
        } else {
            String fileName = "/output" + programId + "/" + identifier + ".csv";
            writeResult(result, fileName);
        }
    }
    /**
     * The writeResult method writes the result to HDFS.
     * It writes the result to HDFS in the specified path.
     * @param data it is the list of KeyValuePair objects to be written.
     * @param path it is the path where the data will be written.
     * @throws IOException if there is an error writing the result.
     */
    private void writeResult(List<KeyValuePair> data, String path) throws IOException {

        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + path);
        Path outputPath = new Path(path);
        FSDataOutputStream outputStream = fs.create(outputPath);
        for (KeyValuePair pair : data) {
            Integer key = pair.key();
            Integer value = pair.value();
            outputStream.write((key + "," + value + "\n").getBytes());
        }
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + path);
        outputStream.close();
    }

    /**
     * The readAndComputeReduce method reads and computes the reduce operation.
     * It reads the data from HDFS and computes the reduce operation on the data.
     * It returns the KeyValuePair object after the reduce operation is computed.
     * It is called in the second phase of the program.
     * @param idx it is the index of the file.
     * @param reduceMessage it is the ReduceOperation object.
     * @param reduce it is the ReduceOperator object.
     * @return the KeyValuePair object.
     * @throws IOException if there is an error reading the data.
     * @throws IllegalArgumentException if the key file is invalid.
     */
    public KeyValuePair readAndComputeReduce(int idx, ReduceOperation reduceMessage, Operator reduce) throws IOException, IllegalArgumentException {
        Integer key = findKey(idx, reduceMessage.getProgramId());
        logger.info(Thread.currentThread().getName() + ": Reading and computing reduce for key: " + key);
        String path = "/program" + reduceMessage.getProgramId() + "/key" + key;
        List<KeyValuePair> result = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            FSDataInputStream in = fs.open(filePath);


            StringBuilder partialTuple = new StringBuilder();
            int count = 0;
            Data data;

            while ((!(data = readFile(in, count)).data().isEmpty()) || !partialTuple.toString().isEmpty()) {

                logger.info(Thread.currentThread().getName() + ": Data is ready to be processed of partition: " + count + " of file: " + filePath.getName());
                String combinedData = data.data().isEmpty() ? partialTuple.toString() : partialTuple.toString() + data.data();


                partialTuple = processData(combinedData, result);
                count++;
                result = reduce.execute(result);
            }
            in.close();
        }
        logger.info(Thread.currentThread().getName() + ": Reduce has been computed for key: " + key);
        if(result.isEmpty()){
            return null;
        }
        else{
            return result.getFirst();
        }
    }   
    /**
     * The findKey method finds the key from the file.
     * It finds the key from the file using the index and the programId.
     * It returns the key from the file.
     * @param idx it is the index of the file.
     * @param programId it is the id of the program.
     * @return the key from the file.
     * @throws IOException if there is an error finding the key.
     */
    private Integer findKey(int idx, String programId) throws IOException {
        String path = "/program" + programId;
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        Pattern pattern = Pattern.compile("key(-?\\d+)");
        Matcher matcher = pattern.matcher(fileStatuses[idx].getPath().getName());

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new IllegalArgumentException("Invalid Key file");
        }
    }
    /**
     * The readFile method reads the file from HDFS.
     * It reads the file from HDFS and returns the data.
     * It reads the data in chunks of BUFFER_SIZE.
     * @param in it is the FSDataInputStream object.
     * @param numPart it is the partition number.
     * @return the Data object.
     * @throws IOException if there is an error reading the file.
     */
    private Data readFile(FSDataInputStream in, int numPart) throws IOException {

        String data = "";

        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        long accumulatedBytesRead = 0;
        DFSInputStream dfsInputStream = (DFSInputStream) in.getWrappedStream();
        long fileLength = dfsInputStream.getFileLength();
        long seekPosition = (long) numPart * BUFFER_SIZE;


        if (seekPosition >= fileLength) {
            return new Data("", true);
        }
        in.seek(seekPosition);
        while (accumulatedBytesRead < BUFFER_SIZE) {
            if ((bytesRead = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
                data += new String(buffer, 0, bytesRead);
                accumulatedBytesRead += bytesRead;
            } else {
                break;
            }
        }
        logger.info(Thread.currentThread().getName() + ": Read " + accumulatedBytesRead + " bytes from file");
        return new Data(data, seekPosition + accumulatedBytesRead >= fileLength);
    }
}
