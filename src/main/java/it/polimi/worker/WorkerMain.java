package it.polimi.worker;


import java.util.Scanner;

public class WorkerMain {

    private static Worker worker;

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);
        boolean exit = false;

        System.out.println("Insert a hostname");
        String hostname = scanner.nextLine();
        System.out.println("Insert a port");
        String portString = scanner.nextLine();
        int port;
        try {
            port = Integer.parseInt(portString);
        } catch (Exception e) {
            System.out.println("Not a number input port");
            scanner.close();
            return;
        }

        worker = new Worker(hostname, port);
        worker.start();

        try {
            while (!exit) {
                System.out.print("Enter command\nstart\nstop\nexit\n");
                String command = scanner.nextLine();

                if (command.equals("exit")) {
                    exit = true; // Set exit to true to exit the loop
                    stopServer();
                } else if (command.equals("stop")) {
                    stopServer();
                } else if (command.equals("start")) {
                    startServer();
                } else {
                    System.out.println("Unknown command. Try again.");
                }
            }
        } catch (Exception e) {
            System.out.println("Program ended");
        }
        scanner.close(); // Close the scanner to avoid resource leak
    }


    private static void startServer() {
        if (!worker.getIsRunning()) {
            Worker w = new Worker(worker.getHostname(), worker.getPort());
            worker = w;
            worker.start();
            System.out.println("Server started.");
        } else {
            System.out.println("Server was already started.");
        }
        return;
    }

    private static void stopServer() {

        if (worker.getIsRunning()) {
            worker.stopServer();
            try {
                worker.join(); // Wait for the thread to complete gracefully
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Server stopped.");
        } else {
            System.out.println("Server was already stopped.");
        }
        return;
    }
}
