package it.polimi.coordinator;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;

import it.polimi.common.Address;
import it.polimi.common.ConfigFileReader;
import it.polimi.common.HadoopFileReadWrite;
import it.polimi.common.KeyValuePair;

public class Coordinator {

    private Integer numPartitions;
    private List<MutablePair<String, String>> operations;
    private List<Socket> clientSockets;
    
    private KeyAssignmentManager keyManager;

    private Map<String, Socket> fileSocketMap;
    private List<String> files;
    private List<String> localFiles;
    private List<Address> addresses;
    private MutablePair<String,String> lastReduce;

    private Integer endedTasks;
    private List<KeyValuePair> finalResult;

    public Coordinator(MutablePair<Integer, List<MutablePair<String, String>>> operations, MutablePair<List<String>, List<Address>> filesAddresses ) {
        this.clientSockets = new ArrayList<>();
        this.operations = operations.getRight();
        this.numPartitions = operations.getLeft();

        this.localFiles = filesAddresses.getLeft();
        this.addresses = filesAddresses.getRight();
        this.files = new ArrayList<>();
         for(int i = 0; i < localFiles.size(); i++){
                files.add("/input/" + new Path(localFiles.get(i)).getName());
        }


        this.fileSocketMap = new HashMap<>();
        this.lastReduce = new MutablePair<>();
        this.keyManager = new KeyAssignmentManager();
        if(this.numPartitions == 0 || this.operations.size() == 0 || this.files.size() == 0 || this.files.size() != this.numPartitions){
            throw new IllegalArgumentException("Invalid input parameters!");
        }
        this.endedTasks = 0;
        this.finalResult = new ArrayList<>();
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
                if(i<addresses.size()){
                    Address a = addresses.get(i);
                    Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                    clientSockets.add(clientSocket);
                    fileSocketMap.put(f, clientSocket);
                }else{
                    throw new RuntimeException("No workers available");
                }
            } catch (IOException e) {
                fileSocketMap.put(f, getNewActiveSocket(new ArrayList<>(addresses)));
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

    public Socket getNewActiveSocket(List<Address> addressesTocheck){
                
        if(addressesTocheck.size() == 0){
            throw new RuntimeException("No workers available");
        }

        Map<Address,Integer> load = new HashMap<>();
        Socket result = null;
        for(Address a: addressesTocheck){
            load.put(a, 0);
        }
        
        for(Socket s:clientSockets){
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            if(load.containsKey(a)){
                load.put(a, load.get(a) + 1);
            }
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
    public void writeResult(List<KeyValuePair> result){
        endedTasks++;
        finalResult.addAll(result);
        if(endedTasks == numPartitions){
            System.out.println("Writing the final result...");
            try{
                ConfigFileReader.writeResult(finalResult);
            }
            catch(Exception e){
                System.out.println("Error while writing the final result");
                System.out.println(e.getMessage());
            }
            HadoopFileReadWrite.deleteFiles();
        }
    }
    public void initializeHadoop(){
        try{
            HadoopFileReadWrite.updloadFiles(localFiles,"/input/");
        }catch(Exception e){
            throw new RuntimeException("Not possible to connect to the HDFS server. Check the address of the server and if it is running!");
        }
    }
}
