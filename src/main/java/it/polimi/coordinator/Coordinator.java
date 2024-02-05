package it.polimi.coordinator;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.Address;

public class Coordinator {

    private int numPartitions;
    private List<MutablePair<String, String>> operations;
    private List<Socket> clientSockets;
    private Map<Address, String> fileToMachineMap;
    private ArrayList<Boolean> processed;

    public Coordinator(MutablePair<Integer, List<MutablePair<String, String>>> operations, Map<Address, String> processed ) {
        this.clientSockets = new ArrayList<>();
        this.operations = operations.getRight();
        this.numPartitions = operations.getLeft();
        this.fileToMachineMap = processed;
        this.processed = new ArrayList<>(Collections.nCopies(fileToMachineMap.size(), false));                
    }

    public List<Socket> getClientSockets() {
        return clientSockets;
    }

    public List<MutablePair<String, String>> getOperations() {
        return operations;
    }

    public int getNumPartitions() {
        return this.numPartitions;
    }

    public ArrayList<Boolean> getProcessed() {
        return this.processed;
    }

    
    public Map<Address, String> getFileToMachineMap() {
            return fileToMachineMap;
    }
    
    public void initializeConnections(List<Address> list) throws Exception {
        for (Address a : list) {
            try {
                Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                clientSockets.add(clientSocket);
            } catch (IOException e) {
                throw new Exception("Not possible to initialize the connections with the workers!");
            }
        }
    }
    public boolean checkChangeKeyReduce(){
        boolean changeKey = false;
        boolean reduce = false;

        for(MutablePair<String,String> m : operations){
            if(m.getLeft().equals("CHANGEKEY")){
                changeKey = true;
            }
            if(m.getLeft().equals("CHANGEKEY")){
                reduce = true;
            }
        }
        return changeKey && reduce;
    }
}
