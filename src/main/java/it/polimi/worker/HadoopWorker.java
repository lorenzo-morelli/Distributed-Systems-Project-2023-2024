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
import it.polimi.worker.models.Data;
import it.polimi.worker.models.Operator;
import it.polimi.common.messages.NormalOperations;

public class HadoopWorker extends HadoopFileManager {

    public HadoopWorker(String address) throws IOException {
        super(address, 131072);
        logger = LogManager.getLogger("it.polimi.Worker");
    }

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

            for (Operator op : operators) {
                result = op.execute(result);
            }
            if ((task.getReduce() && !task.getChangeKey())) {
                if (reduceResult == null) {
                    reduceResult = result.getFirst();
                } else {
                    result.add(reduceResult);
                    assert reduce != null;
                    reduceResult = reduce.execute(result).getFirst();
                }
                workerHandler.processPartitionTask(List.of(reduceResult), task, i, count, data.end() && partialTuple.toString().isEmpty(), partialTuple.toString(), false);
            } else {
                workerHandler.processPartitionTask(result, task, i, count, data.end() && partialTuple.toString().isEmpty(), partialTuple.toString(), true);

            }
            logger.info(Thread.currentThread().getName() + ": Data processed of partition: " + count + " of file: " + task.getPathFiles().get(i));
            result.clear();
            count++;
        }
        if (task.getReduce() && !task.getChangeKey()) {
            workerHandler.processPartitionTask(List.of(reduceResult), task, i, count, true, "", true);
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


    public void writeKeys(String programId, String identifier, KeyValuePair result) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Writing keys to HDFS");
        String fileName = "/output" + programId + "/" + identifier + "/key" + result.key() + ".csv";
        FileSystem fileSystem = initialize();
        writeResult(List.of(result), fileName, fileSystem);
        fileSystem.close();
        logger.info(Thread.currentThread().getName() + ": Keys written to HDFS");
    }

    public void writeKeys(String programId, String identifier, List<KeyValuePair> result, boolean changeKey, boolean reduce) throws IOException {
        if (result.isEmpty()) {
            return;
        }
        FileSystem fileSystem = initialize();

        if (changeKey && reduce) {
            for (KeyValuePair pair : result) {
                String fileName = "/program" + programId + "/key" + pair.key() + "/" + identifier + ".csv";
                writeResult(List.of(pair), fileName, fileSystem);
            }
        } else {
            String fileName = "/output" + programId + "/" + identifier + ".csv";
            writeResult(result, fileName, fileSystem);
        }
        fileSystem.close();
    }

    private void writeResult(List<KeyValuePair> data, String path, FileSystem fileSystem) throws IOException {

        logger.info(Thread.currentThread().getName() + ": Writing to HDFS: " + path);
        Path outputPath = new Path(path);
        FSDataOutputStream outputStream = fileSystem.create(outputPath);
        for (KeyValuePair pair : data) {
            Integer key = pair.key();
            Integer value = pair.value();
            outputStream.write((key + "," + value + "\n").getBytes());
        }
        logger.info(Thread.currentThread().getName() + ": Written to HDFS: " + path);
        outputStream.close();
    }


    public KeyValuePair readAndComputeReduce(int idx, ReduceOperation reduceMessage, Operator reduce) throws IOException, IllegalArgumentException {
        Integer key = findKey(idx, reduceMessage.getProgramId());
        logger.info(Thread.currentThread().getName() + ": Reading and computing reduce for key: " + key);
        System.out.println(Thread.currentThread().getName() + ": Reading and computing reduce for key: " + key);
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

                System.out.println(Thread.currentThread().getName() + ": Data is ready to be processed of partition: " + count + " of key: " + key);

                String combinedData = data.data().isEmpty() ? partialTuple.toString() : partialTuple.toString() + data.data();


                partialTuple = processData(combinedData, result);
                count++;
                result = reduce.execute(result);
            }
        }
        logger.info(Thread.currentThread().getName() + ": Reduce has been computed for key: " + key);
        return result.getFirst();
    }

    private Integer findKey(int idx, String programId) throws IOException {
        String path = "/program" + programId;
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        Pattern pattern = Pattern.compile("key(\\d+)");
        Matcher matcher = pattern.matcher(fileStatuses[idx].getPath().getName());

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new IllegalArgumentException("Invalid Key file");
        }
    }

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
