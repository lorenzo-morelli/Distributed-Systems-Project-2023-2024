package it.polimi.coordinator;

import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final List<ProgramExecutor> executors = new ArrayList<>();
    /**
     * The main method is used to start the coordinator, read the configurations from the file and start the ProgramExecutor for each program.
     * It reads the HDFS address and the operations file path from the user input.
     * @param args Command line arguments (not used).
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
                executors.add(executor);
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

    /**
     * The getNewActiveSocket method returns a new active socket.
     * @param addressesToCheck represents the list of addresses to check.
     * @param machine represents the machine, if it is not null the machine is used to give higher priority to the sockets of the same machine.
     * @return the new active socket.
     * @throws RuntimeException if no workers are available.
     */
    public static synchronized Socket getNewActiveSocket(List<Address> addressesToCheck, String machine){
        logger.info(Thread.currentThread().getName() + ": Search for a new active socket...");
        if (addressesToCheck.isEmpty()) {
            logger.error(Thread.currentThread().getName() + ": No workers available");
            throw new RuntimeException("No workers available");
        }

        machine = (machine == null) ? "" : machine;

        Map<Address, Integer> load = new HashMap<>();
        Socket result;
        for (Address a : addressesToCheck) {
            if (!machine.equals(a.hostname())) {
                load.put(a, 0);
            } else {
                load.put(a, -2);
            }
        }   
        List<Socket> clientSockets = new ArrayList<>();
        for(ProgramExecutor e : executors){
            clientSockets.addAll(e.getClientSockets());
        }
        for (Socket s : clientSockets) {
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            if (load.containsKey(a)) {
                load.put(a, load.get(a) + 1);
            }
        }
    
        Address finalAddress = Collections.min(load.entrySet(), Map.Entry.comparingByValue()).getKey();
        try {
            result = new Socket(finalAddress.hostname(), finalAddress.port());
            logger.info(Thread.currentThread().getName() + ": New active socket found " + finalAddress.hostname() + ":" + finalAddress.port());
            return result;
        } catch (Exception e) {
            logger.warn(Thread.currentThread().getName() + ": Error while creating the new active socket: " + finalAddress.hostname() + ":" + finalAddress.port());
            addressesToCheck.remove(finalAddress);
            return getNewActiveSocket(addressesToCheck, machine);
        }
    }



}
