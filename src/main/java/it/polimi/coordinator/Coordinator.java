package it.polimi.coordinator;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.Address;

public class Coordinator {

    private int numPartitions;
    private List<MutablePair<String, String>> operations;
    private List<Socket> clientSockets;
    
    public Coordinator(MutablePair<Integer, List<MutablePair<String, String>>> operations) {
        this.clientSockets = new ArrayList<>();
        this.operations = operations.getRight();
        this.numPartitions = operations.getLeft();
    }

    public List<Socket> getClientSockets(){
        return clientSockets;
    }
    public List<MutablePair<String, String>> getOperations(){
        return operations;
    }

    public int getNumPartitions() {
        return this.numPartitions;
    }

    public void initializeConnections(List<Address> list) throws Exception{
        for(Address a: list){
            try {
                Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                clientSockets.add(clientSocket);
            } catch (IOException e) {
                throw new Exception("Not possible to initialize the connections with the workers!");
            }
        }
    }


}
