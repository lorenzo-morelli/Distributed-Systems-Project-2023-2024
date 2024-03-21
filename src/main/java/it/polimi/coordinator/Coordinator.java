package it.polimi.coordinator;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.polimi.common.Address;

/**
 * The Coordinator class is the main class of the coordinator module.
 * It is used to start the coordinator, read the configurations from the file and start the ProgramExecutor for each program.
 */
public class Coordinator {
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");

    /**
     * The main method is used to start the coordinator, read the configurations from the file and start the ProgramExecutor for each program.
     * It reads the HDFS address and the operations file path from the user input.
     * @param args represents the arguments of the main method.
     */
    public static void main(String[] args) {

        PropertyConfigurator.configure("conf/log4j.properties");
        String conf_path;
        String address;
        try {
            Scanner scanner = new Scanner(System.in);

            System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
            address = scanner.nextLine();
            address = (address.isEmpty()) ? "hdfs://localhost:9000" : "hdfs://" + address;

            System.out.println("Insert operations file path (default: 'files/configurations.json'): ");
            conf_path = scanner.nextLine();
            conf_path = conf_path.isEmpty() ? "files/configurations.json" : conf_path;

            scanner.close();
        } catch (Exception e) {
            System.out.println("Error while reading input: " + e.getMessage());
            return;
        }

        logger.info("Starting Coordinator");


        try {
            MutablePair<List<String>, List<Address>> configs = CoordinatorFileManager.readConfigurations(new File(conf_path));

            int i = 0;
            String programId;
            for (String f : configs.getLeft()) {
                programId = String.valueOf(i);
                //programId = UUID.randomUUID().toString();
                ProgramExecutor executor = new ProgramExecutor(String.valueOf(i), programId,
                        f,
                        configs.getRight(),
                        new HadoopCoordinator(address)
                );
                executor.start();
                System.out.println("Program " + i + " identified by " + programId);
                logger.info("Program " + i + " identified by " + programId);
                i++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info("Coordinator initialized");
    }
}
