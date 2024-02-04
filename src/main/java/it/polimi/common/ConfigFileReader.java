package it.polimi.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
            e.printStackTrace();
            throw new Exception("Not possible to read the operations file:\n" + file.getAbsolutePath() + "\nCheck the path and the format of the file!");
        }

        return new MutablePair<>(partitions, dataFunctions);
    }

    public static List<Address> readConfigurations(File file) throws Exception {
        int numWorkers = 0;
        List<Address> addresses = new ArrayList<>();

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {
            });
            numWorkers = (int) jsonData.get("numberWorkers");
            ArrayList<?> servers = (ArrayList<?>) jsonData.get("workers");
            for (int i = 0; i < numWorkers; i++) {
                String server = servers.get(i).toString();
                String[] parts = server.split(":");
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                addresses.add(new Address(host, port));
            }
        } catch (Exception e) {
            throw new Exception("Not possible to read the configuration file:\n" + file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
        }
        return new ArrayList<>(addresses);
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
            throw new Exception("Not possible to read the data file:\n" + file.getAbsolutePath().toString() + "\nCheck the path and the format of the file!");
        }

        return keyValuePairs;
    }
}