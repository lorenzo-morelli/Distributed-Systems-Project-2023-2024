package it.polimi.coordinator;

import java.io.File;

import it.polimi.common.ConfigFileReader;

public class CoordinatorMain {
    public static void main(String[] args) {
        
        String conf_path = "files/conf.json";
        String operations_path = "files/operations.json";
        String data_path = "files/data.csv";

        Coordinator coordinator=null;
        try{
            //Initialize coordinator
            coordinator= new Coordinator();
            coordinator.init(
                ConfigFileReader.readConfigurations(new File(conf_path)),
                ConfigFileReader.readOperations(new File(operations_path)),
                ConfigFileReader.readData(new File(data_path))
            );
        }catch(Exception e){
            System.out.println(e);
            System.out.println("Not possible to initialize the coordinator");
            return;
        }
    }    
}
