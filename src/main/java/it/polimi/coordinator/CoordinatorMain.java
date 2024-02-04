package it.polimi.coordinator;

import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import it.polimi.common.Address;
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
            return;        }

        
        try {
            coordinator.initializeConnections(new ArrayList<>(coordinator.getFileToMachineMap().keySet()));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }


        ExecutorService executorService = Executors.newFixedThreadPool(coordinator.getClientSockets().size());
        
        try{
        for (Socket socket : coordinator.getClientSockets()) {
            
            for(int i = 0;i<coordinator.getFileToMachineMap().size();i++){
                Address a = new Address(socket.getInetAddress().getHostName(), socket.getPort());
                if(!coordinator.getProcessed().get(i) && coordinator.getFileToMachineMap().get(a)!= null){
                    coordinator.getProcessed().set(i, true);
                    executorService.submit(new SocketHandler(socket, coordinator.getFileToMachineMap().get(a),coordinator.getOperations()));
                }
            }
        }
        executorService.shutdown();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}
