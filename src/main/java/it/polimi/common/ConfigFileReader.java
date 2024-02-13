package it.polimi.common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import java.nio.file.Files;

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

            files = objectMapper.convertValue(jsonData.get("files"), new TypeReference<List<String>>() {});
            
            List<String> workers = objectMapper.convertValue(jsonData.get("workers"), new TypeReference<List<String>>() {});
            
            for(int i = 0; i < workers.size(); i++){
                String[] parts = workers.get(i).split(":");
                String hostname = parts[0];
                int port = Integer.parseInt(parts[1]);
                addresses.add(new Address(hostname,port));
            }
        } catch (Exception e) {
            throw new Exception("Not possible to read the configuration file:\n" + file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
        }
        return new MutablePair<>(files, addresses);
    }
    
    public static synchronized MutablePair<Boolean, List<KeyValuePair>> readCheckPoint(File file, Boolean phase2) throws Exception {
        List<KeyValuePair> result = new ArrayList<>();
        Boolean end = false;
    
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
    
            result = objectMapper.convertValue(jsonData.get("values"), new TypeReference<List<KeyValuePair>>() {});    
            if(!phase2){
                end = (Boolean) jsonData.get("end");
            }
        } catch (Exception e) {
            throw new Exception("Not possible to read the checkpoint file:\n" + file.getAbsolutePath().toString());
        }
        return new MutablePair<>(end, result);
    }
    

    public static synchronized void createCheckpoint(List<KeyValuePair> result, String fileName, boolean finished, boolean phase2) throws Exception {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("values", result);
    
        if (!phase2) {
            jsonObject.put("end", finished);
        }
        
        String tempFileName = fileName.split("\\.")[0] + "_temp.json";


       // Write the JSON object to a file
        try (FileWriter fileWriter = new FileWriter(tempFileName)) {
            fileWriter.write(jsonObject.toJSONString());
        } catch (IOException e) {
            throw new Exception("Not possible to write the checkpoint file:\n" + tempFileName);
        }

        java.nio.file.Path sourcePath = java.nio.file.Path.of(tempFileName);
        java.nio.file.Path destinationPath = java.nio.file.Path.of(fileName);
        Files.copy(sourcePath, destinationPath, StandardCopyOption.REPLACE_EXISTING);
    
        Files.deleteIfExists(sourcePath);
    }

    public static void writeResult(List<KeyValuePair> finalResult) throws Exception {
        String fileName = "result.csv";

        try (FileWriter fileWriter = new FileWriter(fileName)) {
            for (KeyValuePair pair : finalResult) {
                fileWriter.write(pair.getKey() + "," + pair.getValue() + "\n");
            }
        } catch (IOException e) {
            throw new Exception("Not possible to write the finalResult:\n" + fileName);
        }
    }
    
}