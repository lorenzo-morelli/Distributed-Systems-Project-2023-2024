package it.polimi.coordinator;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.polimi.common.ConfigFileReader;

public class CoordinatorMain {
    private static Coordinator coordinator;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    public static void main(String[] args) {

        PropertyConfigurator.configure("src/log4j.properties");


        String operations_path = "files/operations.json";
        String conf_path = "files/configurations.json";
        
        logger.info("Starting Coordinator");

        try {
            coordinator = new Coordinator(
                ConfigFileReader.readOperations(new File(operations_path)),
                ConfigFileReader.readConfigurations(new File(conf_path))
                );
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info("Coordinator initialized");

        try {
            coordinator.initializeConnections();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info("Coordinator initialized connections");

        try{
            coordinator.initializeHadoop();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        logger.info("Coordinator initialized Hadoop");

        ExecutorService executorService = Executors.newFixedThreadPool(coordinator.getFileSocketMap().size());
        try{
            int i = 0;
            
            for (String f : coordinator.getFileSocketMap().keySet()) {
                executorService.submit(new SocketHandler(coordinator,f,i,CoordinatorPhase.INIT));
                i++;
            }
            executorService.shutdown();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        logger.info("Coordinator initialized SocketHandlers");
    }
}
