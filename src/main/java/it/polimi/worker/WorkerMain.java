package it.polimi.worker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import it.polimi.common.ConfigFileReader;

public class WorkerMain {
    
    private static List<Worker> workers = new ArrayList<>();
    
    public static void main(String[] args) {
        
        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        
        try{
            workers = ConfigFileReader.readConfigurations(new File("files/conf.json"));
        }catch(Exception e){
            System.out.println(e);
            scanner.close(); // Close the scanner to avoid resource leak
            return;
        }
        startAll();

        try{
            while (!exit) {
                System.out.print("Enter command\nstart k\nstop k\nexit\n");
                String command = scanner.nextLine();

                if (command.equals("exit")) {
                    exit = true; // Set exit to true to exit the loop
                    stopAllServers();
                } else if (command.startsWith("stop ")) {
                    try {
                        int k = Integer.parseInt(command.substring(5));
                        stopServer(k);
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        System.out.println("Invalid command format.");
                    }
                } else if (command.startsWith("start ")) {
                    try {
                        int k = Integer.parseInt(command.substring(6));
                        startServer(k);
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        System.out.println("Invalid command format.");
                    }
                } else {
                    System.out.println("Unknown command. Try again.");
                }
            }
        }catch(Exception e){
            System.out.println("Program ended");
        }
        scanner.close(); // Close the scanner to avoid resource leak
    }

    private static void startServer(int k) {
        for (Worker w: workers) {
            if (w.getWorkerId() == k) {
                if(!w.getIsRunning()){
                    workers.remove(w);
                    Worker worker = new Worker(w.getWorkerId(), w.getHostname(), w.getPort());
                    workers.add(worker);
                    worker.start();
                    System.out.println("Server " + k + " started.");
                }else{
                    System.out.println("Server " + k + " was already started.");                    
                }
                return;
            }
        }
        System.out.println("Server " + k + " not found.");
    }

    private static void startAll() {
        for (Worker w: workers) {
            w.start(); 
            System.out.println("Server " + w.getWorkerId() + " started.");           
        }
    }


    private static void stopServer(int k) {
        for (Worker w: workers) {

            if (w.getWorkerId() == k) {
                if(w.getIsRunning()){
                    w.stopServer();
                    try {
                        w.join(); // Wait for the thread to complete gracefully
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Server " + k + " stopped.");
                }else{
                    System.out.println("Server " + k + " was already stopped.");                    
                }
                return;
            }
        }
        System.out.println("Server " + k + " not found.");
    }

    private static void stopAllServers() {
        for (Worker w : workers) {
            if(w.getIsRunning()){
                w.stopServer();
                try {
                    w.join(); // Wait for the thread to complete gracefully
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Server " + w.getWorkerId() + " stopped.");
            }
        }
        System.out.println("All servers stopped.");
    }
}
