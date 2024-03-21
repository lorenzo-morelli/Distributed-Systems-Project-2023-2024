package it.polimi.coordinator;

import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.polimi.common.Address;

/**
 * The CoordinatorFileManager class is used to read the operations and configurations from the file.
 * It contains the readOperations and readConfigurations methods.
 */
public class CoordinatorFileManager {

    private final static Logger logger = LogManager.getLogger("it.polimi.Coordinator");

    /**
     * The readOperations method is used to read the operations from the file.
     * @param file represents the operation file to read.
     * @return a MutablePair containing the list of operations and the list of files to process.
     * @throws Exception if the file is not found or the format is not correct.
     */
    public MutablePair<List<MutablePair<String, String>>, List<String>> readOperations(File file) throws Exception {

        logger.info(Thread.currentThread().getName() + ": Reading operations file: " + file.getAbsolutePath());
        List<MutablePair<String, String>> dataFunctions = new ArrayList<>();
        List<String> files;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<>() {
            });

            List<Map<String, String>> operations = objectMapper.convertValue(jsonData.get("operations"), new TypeReference<>() {
            });
            files = objectMapper.convertValue(jsonData.get("files"), new TypeReference<>() {
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
        logger.info(Thread.currentThread().getName() + ": Operations file read: " + file.getAbsolutePath());
        return new MutablePair<>(dataFunctions, files);
    }
    /**
     * The readConfigurations method is used to read the configurations from the file.
     * @param file represents the configuration file to read.
     * @return a MutablePair containing the list of programs paths and the list of workers addresses.
     * @throws Exception if the file is not found or the format is not correct.
     */
    public static MutablePair<List<String>, List<Address>> readConfigurations(File file) throws Exception {
        logger.info("Reading configuration file: " + file.getAbsolutePath());
        List<Address> addresses = new ArrayList<>();
        List<String> programsPaths;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonData = objectMapper.readValue(file, new TypeReference<>() {
            });

            programsPaths = objectMapper.convertValue(jsonData.get("programs"), new TypeReference<>() {
            });

            List<String> workers = objectMapper.convertValue(jsonData.get("workers"), new TypeReference<>() {
            });

            for (String worker : workers) {
                String[] parts = worker.split(":");
                String hostname = parts[0];
                int port = Integer.parseInt(parts[1]);
                addresses.add(new Address(hostname, port));
            }
        } catch (Exception e) {
            logger.error(e);
            throw new Exception("Not possible to read the configuration file:\n" + file.getAbsolutePath() + "\nCheck the path and the format of the file!");
        }
        logger.info("Configuration file read: " + file.getAbsolutePath());
        return new MutablePair<>(programsPaths, addresses);
    }
}