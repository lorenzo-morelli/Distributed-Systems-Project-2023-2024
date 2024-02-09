package it.polimi.coordinator;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.Address;

public class Coordinator {

    private Integer numPartitions;
    private List<MutablePair<String, String>> operations;
    private List<Socket> clientSockets;
    
    private KeyAssignmentManager keyManager;

    private Map<String, Socket> fileSocketMap;
    private Map<String, Address> fileAddresMap;
    private List<String> files;
    private List<Address> addresses;
    MutablePair<String,String> lastReduce;


    public Coordinator(MutablePair<Integer, List<MutablePair<String, String>>> operations, MutablePair<List<String>, List<Address>> filesAddresses ) {
        this.clientSockets = new ArrayList<>();
        this.operations = operations.getRight();
        this.numPartitions = operations.getLeft();

        this.files = filesAddresses.getLeft();
        this.addresses = filesAddresses.getRight();

        this.fileSocketMap = new HashMap<>();
        this.fileAddresMap = new HashMap<>();
        this.lastReduce = new MutablePair<>();
        this.keyManager = new KeyAssignmentManager();
        if(this.numPartitions == 0 || this.operations.size() == 0 || this.files.size() == 0 || this.addresses.size() != this.numPartitions){
            throw new IllegalArgumentException("Invalid input parameters!");
        }
    }

    public KeyAssignmentManager getKeyManager(){
        return keyManager;
    }
    public List<Socket> getClientSockets() {
        return clientSockets;
    }

    public List<MutablePair<String, String>> getOperations() {
        return operations;
    }

    public Integer getNumPartitions() {
        return this.numPartitions;
    }

    public Map<String, Socket> getFileSocketMap() {
        return fileSocketMap;
    }
    public List<Address> getAddresses(){
        return addresses;
    }      
    public MutablePair<String,String> getLastReduce(){
        return lastReduce;
    }   
    public void initializeConnections(){
        
        int i = 0;
        for (String f : files) {
            try {
                Address a = addresses.get(i);
                Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                clientSockets.add(clientSocket);
                fileSocketMap.put(f, clientSocket);
                fileAddresMap.put(f, a);
            
            } catch (IOException e) {
                fileSocketMap.put(f, getLeastLoadedWorker());
                fileAddresMap.put(f, addresses.get(i));
            }
            i++;
        }

    }
    public boolean checkChangeKeyReduce(){
        boolean changeKey = false;
        boolean reduce = false;
        boolean firstReduce = true;
        for(MutablePair<String,String> m : operations){
            if(m.getLeft().equals("CHANGEKEY")){
                changeKey = true;
            }
            if(m.getLeft().equals("REDUCE")){
                reduce = true;
                if(firstReduce){
                    lastReduce = m;
                    firstReduce = false;
                }
            }
        }
        return changeKey && reduce;
    }

    public void retryConnection(String f) {
        
        try {
            System.out.println("Retrying connection for file: " + f);
            Address a =  fileAddresMap.get(f);

            Socket clientSocket = new Socket(a.getHostname(), a.getPort());
            clientSockets.add(clientSocket);
            fileSocketMap.put(f, clientSocket);
        
        } catch (IOException e) {
            if(clientSockets.size() == 0){
                throw new RuntimeException("No workers available");
            }
            fileSocketMap.put(f, getLeastLoadedWorker());
        }
    }

    private Socket getLeastLoadedWorker(){
        
        if(clientSockets.size() == 0){
            return null;
        }

        Map<Socket,Integer> load = new HashMap<>();
        for(Socket s : clientSockets){
            load.put(s, 0);
        }

        for(String f : files){
            if(fileSocketMap.get(f) != null){
                load.put(fileSocketMap.get(f), load.get(fileSocketMap.get(f)) + 1);
            }
        }
        Socket s = Collections.min(load.entrySet(), Map.Entry.comparingByValue()).getKey();
        try{
            return new Socket(s.getInetAddress().getHostName(), s.getPort());
        }catch(Exception e){
            clientSockets.remove(s);
            throw new RuntimeException("Not possible to initialize connection");
        }
    }
    public Socket getNewActiveSocket(List<Address> addressesTocheck){
        
        if(addressesTocheck.size() == 0 || clientSockets.size() == 0){
            throw new RuntimeException("No workers available");
        }

        Map<Address,Integer> load = new HashMap<>();
        Socket result = null;
        for(Address a: addressesTocheck){
            load.put(a, 0);
        }
        for(Socket s:clientSockets){
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            load.put(a, load.get(a) + 1);
        }
        Address finalAddress = Collections.min(load.entrySet(), Map.Entry.comparingByValue()).getKey();
        
        try{
            result = new Socket(finalAddress.getHostname(), finalAddress.getPort());
            return result;
        }catch(Exception e){
            addressesTocheck.remove(finalAddress);
            return getNewActiveSocket(addressesTocheck);
        }
    }

}
