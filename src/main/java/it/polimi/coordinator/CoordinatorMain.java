package it.polimi.coordinator;

import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import it.polimi.common.Address;
import it.polimi.common.ConfigFileReader;
import it.polimi.common.messages.Task;

public class CoordinatorMain {
    public static void main(String[] args) {

        String operations_path = "files/operations.json";

        Coordinator coordinator;
        try {
            coordinator = new Coordinator(ConfigFileReader.readOperations(new File(operations_path)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ArrayList<Address> addresses;
        try {
            addresses = readAddresses(coordinator.getNumPartitions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            coordinator.initializeConnections(addresses);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        ExecutorService executorService = Executors.newFixedThreadPool(coordinator.getClientSockets().size());
        for (Socket socket : coordinator.getClientSockets()) {
            executorService.submit(new SocketHandler(socket));
        }
        executorService.shutdown();
    }

    public static ArrayList<Address> readAddresses(int numPartitions) {
        Scanner scanner = new Scanner(System.in);
        ArrayList<Address> addresses = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            System.out.println("Insert a hostname");
            String hostname = scanner.nextLine();
            System.out.println("Insert a port");
            String portString = scanner.nextLine();
            int port = Integer.parseInt(portString);
            addresses.add(new Address(hostname, port));
        }
        return addresses;
    }
}
