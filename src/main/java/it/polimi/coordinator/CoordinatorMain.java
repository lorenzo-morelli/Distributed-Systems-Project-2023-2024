package it.polimi.coordinator;

import java.io.File;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.polimi.common.ConfigFileReader;
import it.polimi.common.HadoopFileReadWrite;

public class CoordinatorMain {
    private static Coordinator coordinator;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    public static void main(String[] args) {

        PropertyConfigurator.configure("conf/log4j.properties");

        Scanner scanner = new Scanner(System.in);

        System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
        String address = scanner.nextLine();
        if(!address.equals(""))
            HadoopFileReadWrite.setHDFS_URI("hdfs://" + address);

        System.out.println("Insert operations file path (default: 'files/operations.json'): ");
        String temp = scanner.nextLine();
        String operations_path = temp.equals("") ? "files/operations.json" : temp;

        System.out.println("Insert operations file path (default: 'files/configurations.json'): ");
        temp = scanner.nextLine();
        String conf_path = temp.equals("") ? "files/configurations.json" : temp;

        scanner.close();
        

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
            return;
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
            return;
        }
        logger.info("Coordinator initialized SocketHandlers");
    }
}
