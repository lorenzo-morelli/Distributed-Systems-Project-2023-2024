package it.polimi.coordinator;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import it.polimi.common.Address;
import it.polimi.common.KeyValuePair;

public class ProgramExecutor extends Thread{

    private Integer numPartitions;
    private List<MutablePair<String, String>> operations;
    private List<Socket> clientSockets;
    
    private KeyAssignmentManager keyManager;

    private Map<String, Socket> fileSocketMap;
    private List<String> files;
    private List<String> localFiles;
    private List<Address> addresses;
    private MutablePair<String,String> lastReduce;
    private CoordinatorFileManager coordinatorFileManager;

    private Integer endedTasks;
    private List<KeyValuePair> finalResult;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private String programId;
    private String op_path;
    private HadoopCoordinator hadoopCoordinator;
    public ProgramExecutor(String programId,String op_path,List<Address> addresses, HadoopCoordinator hadoopCoordinator) {
        this.clientSockets = new ArrayList<>();
        this.programId = programId;
        this.op_path = op_path;
       
        this.addresses = addresses;
       
        
        this.coordinatorFileManager = new CoordinatorFileManager();

        this.fileSocketMap = new HashMap<>();
        this.lastReduce = new MutablePair<>();
        this.keyManager = new KeyAssignmentManager();
        
        this.hadoopCoordinator = hadoopCoordinator;
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
    public String getProgramId(){
        return programId;
    }
    public HadoopCoordinator getHadoopCoordinator(){
        return hadoopCoordinator;
    }

    private void initializeConnections(){
        logger.info(Thread.currentThread().getName()+ ": Initializing connections...");
        int i = 0;
        for (String f : files) {
            try {
                if(i<addresses.size()){
                    Address a = addresses.get(i);
                    Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                    clientSockets.add(clientSocket);
                    fileSocketMap.put(f, clientSocket);
                }else{
                    try{
                        fileSocketMap.put(f, getNewActiveSocket(new ArrayList<>(addresses),null));
                    }catch(Exception e){
                        logger.error(Thread.currentThread().getName()+ ": No workers available");
                        throw new RuntimeException("No workers available");
                    }
                }
            } catch (IOException e) {
                fileSocketMap.put(f, getNewActiveSocket(new ArrayList<>(addresses),null));
            }
            i++;
        }
        logger.info(Thread.currentThread().getName()+ ": Connections initialized");

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
        logger.info(Thread.currentThread().getName() + ": Phase 2 exists:" + (changeKey && reduce));
        return changeKey && reduce;
    }

    public Socket getNewActiveSocket(List<Address> addressesTocheck, String machine){
        logger.info(Thread.currentThread().getName()+ ": Search for a new active socket...");
        if(addressesTocheck.size() == 0){
            logger.error(Thread.currentThread().getName()+ ": No workers available");
            throw new RuntimeException("No workers available");
        }
        
        machine = (machine == null) ? "" : machine;
        
        Map<Address,Integer> load = new HashMap<>();
        Socket result = null;
        for(Address a: addressesTocheck){
            if(!machine.equals(a.getHostname())){
                load.put(a, 0);
            }else{
                load.put(a, -1);
            }
        }
        
        for(Socket s:clientSockets){
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            if(load.containsKey(a) && !a.getHostname().equals(machine)){
                load.put(a, load.get(a) + 1);
            }
        }
        Address finalAddress = Collections.min(load.entrySet(), Map.Entry.comparingByValue()).getKey();
        
        try{
            result = new Socket(finalAddress.getHostname(), finalAddress.getPort());
            logger.info(Thread.currentThread().getName()+ ": New active socket found " + finalAddress.getHostname() + ":" + finalAddress.getPort());
            return result;
        }catch(Exception e){
            logger.warn(Thread.currentThread().getName() + ": Error while creating the new active socket: " + finalAddress.getHostname() + ":" + finalAddress.getPort());
            addressesTocheck.remove(finalAddress);
            return getNewActiveSocket(addressesTocheck, machine);
        }
    }
    public void writeResult(List<KeyValuePair> result){
        endedTasks++;
        finalResult.addAll(result);
        if(endedTasks == numPartitions){
            System.out.println(Thread.currentThread().getName()+ ": Writing the final result...");
            logger.info(Thread.currentThread().getName() + ": Writing the final result...");
            try{
                coordinatorFileManager.writeResult(programId,finalResult);
                logger.info(Thread.currentThread().getName() +": Final result written");
            }
            catch(Exception e){
                System.out.println(Thread.currentThread().getName() + ": Error while writing the final result");
                logger.error(Thread.currentThread().getName()+ ": Error while writing the final result");
                System.out.println(e.getMessage());
            }
            hadoopCoordinator.deleteFiles(programId);
            logger.info(Thread.currentThread().getName() + ": Closing file system");
            hadoopCoordinator.closeFileSystem();
            logger.info(Thread.currentThread().getName() + ": File system closed");
        }
    }
    private void initializeHadoop(){
        try{
            logger.info(Thread.currentThread().getName()+ ": Uploading files to HDFS...");
            hadoopCoordinator.uploadFiles(localFiles,"/input" + programId + "/");
            logger.info(Thread.currentThread().getName() + ": Files uploaded to HDFS successfully");
        }catch(Exception e){
            logger.error(Thread.currentThread().getName() + ": Error while uploading files to HDFS\n" + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }
    private boolean readOperations() throws Exception{


        MutablePair<List<MutablePair<String, String>>,List<String>> operations = coordinatorFileManager.readOperations(new File(op_path));  

        this.operations = operations.getLeft();

        this.localFiles = operations.getRight();
        this.files = new ArrayList<>();
         for(int i = 0; i < localFiles.size(); i++){
                files.add("/input"+programId+"/" + new Path(localFiles.get(i)).getName());
        }
        this.numPartitions = files.size();
        
        if(this.numPartitions == 0 || this.operations.size() == 0){
            logger.info(Thread.currentThread().getName() + ": Operations or num partitions are 0!");
            return false;
        }
        return true;
    }

    @Override
    public void run(){

        Thread.currentThread().setName("ProgramExecutor" + programId);

        try{
            if(!this.readOperations()){
                return;
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
            return;
        }
       

        try {
            this.initializeConnections();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized connections");

        try{
            this.initializeHadoop();
        }catch(Exception e){
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized Hadoop");

        ExecutorService executorService = Executors.newFixedThreadPool(this.getFileSocketMap().size());
        try{
            int i = 0;
            
            for (String f : this.getFileSocketMap().keySet()) {
                executorService.submit(new SocketHandler(this,f,i,CoordinatorPhase.INIT));
                i++;
            }
            executorService.shutdown();
        }catch(Exception e){
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized SocketHandlers");
    }
}
