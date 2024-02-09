package it.polimi.coordinator;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import it.polimi.common.ConfigFileReader;

public class CoordinatorMain {
    private static Coordinator coordinator;

    public static void main(String[] args) {

        String operations_path = "files/operations.json";
        String conf_path = "files/configurations.json";
        try {
            coordinator = new Coordinator(
                ConfigFileReader.readOperations(new File(operations_path)),
                ConfigFileReader.readConfigurations(new File(conf_path))
                );
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        try {
            coordinator.initializeConnections();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }


        ExecutorService executorService = Executors.newFixedThreadPool(coordinator.getFileSocketMap().size());

        try{
            int i = 0;
            
            for (String f : coordinator.getFileSocketMap().keySet()) {
                executorService.submit(new SocketHandler(coordinator,f,i,CoordinatorPhase.INIT,false));
                i++;
            }
            executorService.shutdown();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}
