package it.polimi.coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;



import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.polimi.common.Address;
import it.polimi.common.KeyValuePair;


public class CoordinatorFileManager {
    
    private final static Logger logger = LogManager.getLogger("it.polimi.Coordinator");

    public static MutablePair<List<MutablePair<String, String>>,List<String>> readOperations(File file) throws Exception {

        logger.info("Reading operations file: " + file.getAbsolutePath().toString());
        List<MutablePair<String, String>> dataFunctions = new ArrayList<>();
        List<String> files = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {
            });

            List<Map<String, String>> operations = objectMapper.convertValue(jsonData.get("operations"), new TypeReference<List<Map<String, String>>>() {
            });
            files = objectMapper.convertValue(jsonData.get("files"), new TypeReference<List<String>>() {
            });
            for (Map<String, String> operation : operations) {
                String operator = operation.get("operator");
                String function = operation.get("function");
                dataFunctions.add(new MutablePair<>(operator, function));
            }

        } catch (Exception e) {
            logger.error(e);
            throw new Exception("Not possible to read the operations file:\n" + file.getAbsolutePath() + "\nCheck the path and the format of the file!");
        }
        logger.info("Operations file read: " + file.getAbsolutePath().toString());
        return new MutablePair<>(dataFunctions,files);
    }

    public static MutablePair<List<String>, List<Address>> readConfigurations(File file) throws Exception {
        logger.info("Reading configuration file: " + file.getAbsolutePath().toString());
        List<Address> addresses = new ArrayList<>();
        List<String> programsPaths = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});

            programsPaths = objectMapper.convertValue(jsonData.get("programs"), new TypeReference<List<String>>() {});
            
            List<String> workers = objectMapper.convertValue(jsonData.get("workers"), new TypeReference<List<String>>() {});
            
            for(int i = 0; i < workers.size(); i++){
                String[] parts = workers.get(i).split(":");
                String hostname = parts[0];
                int port = Integer.parseInt(parts[1]);
                addresses.add(new Address(hostname,port));
            }
        } catch (Exception e) {
            logger.error(e);
            throw new Exception("Not possible to read the configuration file:\n" + file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
        }
        logger.info("Configuration file read: " + file.getAbsolutePath().toString());
        return new MutablePair<>(programsPaths, addresses);
    }
    

    public static void writeResult(Integer programId,List<KeyValuePair> finalResult) throws IOException {
        String fileName = "result-"+programId+".csv";

        try (FileWriter fileWriter = new FileWriter(fileName)) {
            for (KeyValuePair pair : finalResult) {
                fileWriter.write(pair.getKey() + "," + pair.getValue() + "\n");
            }
        } catch (IOException e) {
            throw new IOException("Not possible to write the finalResult:\n" + fileName);
        }
    }
   
    
}