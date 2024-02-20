package it.polimi.coordinator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.polimi.common.Address;
import it.polimi.common.HadoopFileManager;

public class Coordinator {
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    public static void main(String[] args) {

        PropertyConfigurator.configure("conf/log4j.properties");
        String conf_path;
        try{
            Scanner scanner = new Scanner(System.in);

            System.out.println("Insert HDFS address (default: 'localhost:9000'): ");
            String address = scanner.nextLine();
            if(!address.equals(""))
                HadoopFileManager.setHDFS_URI("hdfs://" + address);

            String temp;
            System.out.println("Insert operations file path (default: 'files/configurations.json'): ");
            temp = scanner.nextLine();
            conf_path = temp.equals("") ? "files/configurations.json" : temp;

            scanner.close();
        }catch(Exception e){
            System.out.println("Error while reading input: " + e.getMessage());
            return;
        }

        logger.info("Starting Coordinator");

       
        
        ArrayList<ProgramExecutor> coordinators = new ArrayList<>();

        try {
            MutablePair<List<String>, List<Address>> configs = CoordinatorFileManager.readConfigurations(new File(conf_path));

            int programId = 0;
            for(String f : configs.getLeft()){
                coordinators.add(new ProgramExecutor(programId,
                    CoordinatorFileManager.readOperations(new File(f)),
                    configs.getRight()
                    ));
                programId++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info("Coordinator initialized");


        for(ProgramExecutor c : coordinators){
            c.start();
        }
    }   
}
