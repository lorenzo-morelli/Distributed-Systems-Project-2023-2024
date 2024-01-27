package it.polimi.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.polimi.worker.Worker;

public class ConfigFileReader {

    public static Pair<Integer, Object[]> readOperations(File file) throws Exception {
        int partitions = 0;
        Object[] objects = null;
    
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
            partitions = (int) jsonData.get("partitions");
    
            objects = (Object[]) jsonData.get("operations");
    
            for (int i = 0; i < objects.length; i++) {
                System.out.println((String) objects[i]); 
            }
    
        } catch (Exception e) {
            throw new Exception("Not possible to read the operations file:\n"+file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
        }
    
        return new MutablePair<>(partitions, objects);
    }

    public static List<Worker> readConfigurations(File file) throws Exception {
        int numWorkers = 0;
        List<Worker> workers = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>(){});
            numWorkers = (int) jsonData.get("workers");
            ArrayList<?> servers = (ArrayList<?>) jsonData.get("servers");  
            for (int i = 0; i < numWorkers; i++) {
                String server = servers.get(i).toString();
                String[] parts = server.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                Worker worker = new Worker(i, host, port);
                workers.add(worker);
            }
        } catch (Exception e) {
            throw new Exception("Not possible to read the configuration file:\n"+file.getAbsolutePath().toString()+"\nCheck the path and the format of the file!");
        }
        return new ArrayList<>(workers);
    }

    public static List<KeyValuePair> readData(File file) throws Exception {
        List<KeyValuePair> keyValuePairs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    Integer key = Integer.parseInt(parts[0].trim());
                    Integer value = Integer.parseInt(parts[1].trim());
                    keyValuePairs.add(new KeyValuePair(key, value));
                } else {
                    System.out.println("Invalid line in CSV: " + line);
                }
            }
        } catch (Exception e) {
            throw new Exception("Not possible to read the data file:\n"+file.getAbsolutePath().toString()+"\nCheck the path and the format of the file!");
        }

        return keyValuePairs;
    }
}