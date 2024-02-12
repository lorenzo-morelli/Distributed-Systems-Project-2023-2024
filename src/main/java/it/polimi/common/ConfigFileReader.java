package it.polimi.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.shaded.json.JSONObject;

public class ConfigFileReader {

    public static MutablePair<Integer, List<MutablePair<String, String>>> readOperations(File file) throws Exception {
        int partitions = 0;
        List<MutablePair<String, String>> dataFunctions = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {
            });
            partitions = (int) jsonData.get("partitions");

            List<Map<String, String>> operations = objectMapper.convertValue(jsonData.get("operations"), new TypeReference<List<Map<String, String>>>() {
            });

            for (Map<String, String> operation : operations) {
                String operator = operation.get("operator");
                String function = operation.get("function");
                dataFunctions.add(new MutablePair<>(operator, function));
            }

        } catch (Exception e) {
            throw new Exception("Not possible to read the operations file:\n" + file.getAbsolutePath() + "\nCheck the path and the format of the file!");
        }

        return new MutablePair<>(partitions, dataFunctions);
    }

    public static MutablePair<List<String>, List<Address>> readConfigurations(File file) throws Exception {
        List<Address> addresses = new ArrayList<>();
        List<String> files = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});

            List<String> tempFiles = objectMapper.convertValue(jsonData.get("files"), new TypeReference<List<String>>() {});
            
            HadoopFileReadWrite.updloadFiles(tempFiles,"/input/");

            List<String> workers = objectMapper.convertValue(jsonData.get("workers"), new TypeReference<List<String>>() {});
            
            for(int i = 0; i < workers.size(); i++){
                String[] parts = workers.get(i).split(":");
                String hostname = parts[0];
                int port = Integer.parseInt(parts[1]);
                addresses.add(new Address(hostname,port));
            }
            for(int i = 0; i < tempFiles.size(); i++){
                files.add("/input/" + new Path(tempFiles.get(i)).getName());
            }
        } catch (Exception e) {
            if(e instanceof ConnectException){
                throw new Exception("Not possible to connect to the HDFS server. Check if the server is running!");
            }else{
                throw new Exception("Not possible to read the configuration file:\n" + file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
            }
        }
        return new MutablePair<>(files, addresses);
    }
    
    public static synchronized MutablePair<Boolean, List<KeyValuePair>> readCheckPoint(File file) throws Exception {
        List<KeyValuePair> result = new ArrayList<>();
        Boolean end = false;
    
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
    
            result = objectMapper.convertValue(jsonData.get("values"), new TypeReference<List<KeyValuePair>>() {});    
    
            end = (Boolean) jsonData.get("end");
    
        } catch (Exception e) {
            throw new Exception("Not possible to read the checkpoint file:\n" + file.getAbsolutePath().toString() + "!");
        }
        return new MutablePair<>(end, result);
    }
    

    public static synchronized void createCheckpoint(List<KeyValuePair> result, String fileName, boolean finished) throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("values", result);
        jsonObject.put("end", finished);

        // Write the JSON object to a file
        try (FileWriter fileWriter = new FileWriter(fileName)) {
            fileWriter.write(jsonObject.toJSONString());
        } catch (IOException e) {
            throw new Exception("Not possible to write the checkpoint file:\n" + fileName + "!");
        }
    }
}