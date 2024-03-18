package it.polimi.coordinator;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.polimi.common.Address;

public class Coordinator {
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    public static void main(String[] args) {

        PropertyConfigurator.configure("conf/log4j.properties");
        String conf_path;
        String address;
        try{
            Scanner scanner = new Scanner(System.in);

            System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
            address = scanner.nextLine();
            address = (address.equals("")) ? "hdfs://localhost:9000" : "hdfs://" + address;

            System.out.println("Insert operations file path (default: 'files/configurations.json'): ");
            conf_path = scanner.nextLine();
            conf_path = conf_path.equals("") ? "files/configurations.json" : conf_path;

            scanner.close();
        }catch(Exception e){
            System.out.println("Error while reading input: " + e.getMessage());
            return;
        }

        logger.info("Starting Coordinator");

       

        try {
            MutablePair<List<String>, List<Address>> configs = CoordinatorFileManager.readConfigurations(new File(conf_path));

            int i = 0;
            String programId;
            for(String f : configs.getLeft()){
                programId = UUID.randomUUID().toString();
                ProgramExecutor executor = new ProgramExecutor(String.valueOf(i),programId,
                    f,
                    configs.getRight(),
                    new HadoopCoordinator(address)
                    );
                executor.start();
                System.out.println("Program " + i + " identified by " + programId );
                logger.info("Program " + i + " identified by " + programId );    
                i++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info("Coordinator initialized");        
    }   
}
