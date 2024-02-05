package it.polimi.coordinator;

import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
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
            coordinator.initializeConnections(new ArrayList<>(coordinator.getAddressFileMap().keySet()));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }



        KeyAssignmentManager keyManager = new KeyAssignmentManager();
        ExecutorService executorService = Executors.newFixedThreadPool(coordinator.getClientSockets().size());
        
        try{
        for (Socket socket : coordinator.getClientSockets()) {
            executorService.submit(new SocketHandler(socket, 
                coordinator.getSocketFileMap().get(socket),
                coordinator.getOperations(),
                coordinator.checkChangeKeyReduce(),
                coordinator.getNumPartitions(),
                keyManager));
        }
        executorService.shutdown();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}
