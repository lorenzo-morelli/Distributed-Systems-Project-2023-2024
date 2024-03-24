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

/**
 * The ProgramExecutor class is responsible for executing the program.
 * It contains methods to manage the files per worker, initialize the connections, initialize Hadoop, read the operations and manage the end of the program.
 */

public class ProgramExecutor extends Thread {

    private int endedWorkers;
    private List<MutablePair<String, String>> operations;
    private final List<Socket> clientSockets;
    private final KeyAssignmentManager keyManager;
    private final Map<List<String>, Socket> fileSocketMap;
    private List<String> files;
    private List<String> localFiles;
    private final List<Address> addresses;
    private MutablePair<String, String> lastReduce;
    private final CoordinatorFileManager coordinatorFileManager;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private final String programId;
    private final String outputId;
    private final String op_path;
    private final HadoopCoordinator hadoopCoordinator;
    private volatile boolean errorPresent;
    private boolean changeKey;
    private boolean reduce;

    /**
     * The ProgramExecutor class constructor creates a new ProgramExecutor.
     * @param output_id represents the number of the program.
     * @param programId represents the program id.
     * @param op_path represents the path of the operations file.
     * @param addresses represents the list of addresses.
     * @param hadoopCoordinator represents the hadoop coordinator.
     */

    public ProgramExecutor(String output_id, String programId, String op_path, List<Address> addresses, HadoopCoordinator hadoopCoordinator) {
        this.clientSockets = new ArrayList<>();
        this.programId = programId;
        this.op_path = op_path;
        this.outputId = output_id;
        this.addresses = addresses;


        this.coordinatorFileManager = new CoordinatorFileManager();

        this.fileSocketMap = new HashMap<>();
        this.lastReduce = new MutablePair<>();
        this.keyManager = new KeyAssignmentManager(hadoopCoordinator, programId);

        this.hadoopCoordinator = hadoopCoordinator;

        this.errorPresent = false;
        this.endedWorkers = 0;
        this.changeKey = false;
        this.reduce = false;

    }
    /**
     * The IsErrorPresent method returns if there is an error in the program.
     * @return the error present flag.
     */
    public boolean IsErrorPresent() {
        return errorPresent;
    }
    /**
     * The setErrorPresent method sets the error present flag.
     * @param errorPresent represents the error present flag.
     */
    public void setErrorPresent(boolean errorPresent) {
        this.errorPresent = errorPresent;
    }
    /**
     * The getKeyManager method returns the key manager.
     * @return the key manager.
     */
    public KeyAssignmentManager getKeyManager() {
        return keyManager;
    }
    /**
     * The getClientSockets method returns the client sockets.
     * @return the client sockets.
     */
    public List<Socket> getClientSockets() {
        return clientSockets;
    }
    /**
     * The getOperations method returns the operations of the program.
     * @return the operations.
     */
    public List<MutablePair<String, String>> getOperations() {
        return operations;
    }
    /**
     * The getNumPartitions method returns the number of partitions (number of addresses of the workers).
     * @return the number of partitions.
     */
    public int getNumPartitions() {
        return this.addresses.size();
    }
    /**
     * The getFileSocketMap method returns the file socket map.
     * @return the file socket map.
     */
    public Map<List<String>, Socket> getFileSocketMap() {
        return fileSocketMap;
    }
    /**
     * The getAddresses method returns the list of addresses of the workers.
     * @return the list of addresses of the workers.
     */
    public List<Address> getAddresses() {
        return addresses;
    }
    /**
     * The getLastReduce method returns the last reduce operation.
     * @return the last reduce operation.
     */
    public MutablePair<String, String> getLastReduce() {
        return lastReduce;
    }
    /**
     * The getProgramId method returns the program id.
     * @return the program id.
     */
    public String getProgramId() {
        return programId;
    }
    /**
     * The getChangeKey method returns the change key flag which indicates if the change key operation is present or not.
     * @return the change key flag which indicates if the change key operation is present or not.
     */
    public boolean getChangeKey() {
        return changeKey;
    }
    /**
     * The getReduce method returns the reduce flag which indicates if the reduce operation is present or not.
     * @return the reduce flag which indicates if the reduce operation is present or not.
     */
    public boolean getReduce() {
        return reduce;
    }
    /**
     * The getFilesSize method returns the size of the files.
     * @return the size of the files.
     */
    public int getFilesSize() {
        return files.size();
    }
    /**
     * The manageFilesPerWorker method returns the files for a specific worker.
     * @param workerIndex represents the index of the worker.
     * @param local represents if the path of the files is local or not.
     * @return the list of files for a specific worker.
     */
    private List<String> manageFilesPerWorker(int workerIndex, boolean local) {
        int numFilesPerWorker = files.size() / addresses.size();
        int remainingFiles = files.size() % addresses.size();

        int start = workerIndex * numFilesPerWorker + Math.min(workerIndex, remainingFiles);
        int end = (workerIndex + 1) * numFilesPerWorker + Math.min(workerIndex + 1, remainingFiles);

        List<String> filesPerWorker = new ArrayList<>();
        if (!local) {
            for (int i = start; i < end && i < files.size(); i++) {
                filesPerWorker.add(files.get(i));
            }
        } else {
            for (int i = start; i < end && i < localFiles.size(); i++) {
                filesPerWorker.add(localFiles.get(i));
            }
        }
        return filesPerWorker;
    }
    /**
     * The initializeConnections method initializes the connections with the workers.
     */
    private void initializeConnections() {
        logger.info(Thread.currentThread().getName() + ": Initializing connections...");


        for (int j = 0; j < Math.min(addresses.size(), files.size()); j++) {

            List<String> filesWorker = manageFilesPerWorker(j, false);
            try {
                Address a = addresses.get(j);
                Socket clientSocket = new Socket(a.hostname(), a.port());
                clientSockets.add(clientSocket);
                fileSocketMap.put(filesWorker, clientSocket);
            } catch (IOException e) {
                Socket clientSocket = getNewActiveSocket(new ArrayList<>(addresses), null);
                clientSockets.add(clientSocket);
                fileSocketMap.put(filesWorker, clientSocket);
            }

        }
        logger.info(Thread.currentThread().getName() + ": Connections initialized");

    }
    /**
     * The getNewActiveSocket method returns a new active socket.
     * @param addressesToCheck represents the list of addresses to check.
     * @param machine represents the machine, if it is not null the machine is used to give higher priority to the sockets of the same machine.
     * @return the new active socket.
     * @throws RuntimeException if no workers are available.
     */
    public Socket getNewActiveSocket(List<Address> addressesToCheck, String machine) {
        logger.info(Thread.currentThread().getName() + ": Search for a new active socket...");
        if (addressesToCheck.isEmpty()) {
            logger.error(Thread.currentThread().getName() + ": No workers available");
            throw new RuntimeException("No workers available");
        }

        machine = (machine == null) ? "" : machine;

        Map<Address, Integer> load = new HashMap<>();
        Socket result;
        for (Address a : addressesToCheck) {
            if (!machine.equals(a.hostname())) {
                load.put(a, 0);
            } else {
                load.put(a, -2);
            }
        }

        for (Socket s : clientSockets) {
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            if (load.containsKey(a)) {
                load.put(a, load.get(a) + 1);
            }
        }
        Address finalAddress = Collections.min(load.entrySet(), Map.Entry.comparingByValue()).getKey();

        try {
            result = new Socket(finalAddress.hostname(), finalAddress.port());
            logger.info(Thread.currentThread().getName() + ": New active socket found " + finalAddress.hostname() + ":" + finalAddress.port());
            return result;
        } catch (Exception e) {
            logger.warn(Thread.currentThread().getName() + ": Error while creating the new active socket: " + finalAddress.hostname() + ":" + finalAddress.port());
            addressesToCheck.remove(finalAddress);
            return getNewActiveSocket(addressesToCheck, machine);
        }
    }
    /**
     * The initializeHDFS method initializes the Hadoop file system and uploads the files to HDFS.
     * If the program has a reduce operation and the change key operation is not present, the files are uploaded to HDFS without merging them.
     * Otherwise, the files are uploaded to HDFS and merged.
     * @throws RuntimeException if an error occurs while uploading files to HDFS.
     */
    private void initializeHDFS() {
        try {
            if (reduce && !changeKey) {
                hadoopCoordinator.uploadFiles(localFiles, "/input" + programId + "/");

            } else {
                System.out.println(Thread.currentThread().getName() + ": Uploading files to HDFS");
                for (int i = 0; i < Math.min(addresses.size(), files.size()); i++) {


                    List<String> filesPerWorker = manageFilesPerWorker(i, true);

                    hadoopCoordinator.uploadMergedFiles(programId, String.valueOf(i), filesPerWorker);
                }


                this.fileSocketMap.clear();
                for (int i = 0; i < Math.min(addresses.size(), files.size()); i++) {
                    this.fileSocketMap.put(new ArrayList<>(List.of("/input" + programId + "/task" + i + ".csv")), clientSockets.get(i));
                }
            }
        } catch (Exception e) {
            logger.error(Thread.currentThread().getName() + ": Error while uploading files to HDFS\n" + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }
    /**
     * The readOperations method reads the operations from the file.
     * @return true if the operations are read correctly, false otherwise.
     * @throws Exception if an error occurs while reading the operations.
     */
    private boolean readOperations() throws Exception {


        MutablePair<List<MutablePair<String, String>>, List<String>> operations = coordinatorFileManager.readOperations(new File(op_path));

        this.operations = operations.getLeft();
        this.localFiles = operations.getRight();
        this.files = new ArrayList<>();
        for (String localFile : localFiles) {
            files.add("/input" + programId + "/" + new Path(localFile).getName());
        }

        if (this.addresses.isEmpty() || this.operations.isEmpty()) {
            logger.info(Thread.currentThread().getName() + ": Operations or num partitions are 0!");
            System.out.println(Thread.currentThread().getName() + ": Operations or num partitions are 0!");
            return false;
        }
        boolean firstReduce = true;
        for (MutablePair<String, String> m : this.operations) {
            if (m.getLeft().equals("CHANGEKEY")) {
                changeKey = true;
            }
            if (m.getLeft().equals("REDUCE")) {
                reduce = true;
                if (firstReduce) {
                    lastReduce = m;
                    firstReduce = false;
                }
            }
        }

        return true;
    }
    /**
     * The run method executes the program.
     * It initializes the connections, initializes Hadoop, reads the operations and launches the SocketHandlers.
     */
    @Override
    public void run() {

        Thread.currentThread().setName("ProgramExecutor" + programId);

        try {
            if (!this.readOperations()) {
                return;
            }
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + ": Error while reading operations");
            System.out.println(e.getMessage());
            return;
        }


        try {
            this.initializeConnections();
        } catch (Exception e) {

            System.out.println(Thread.currentThread().getName() + ": Error while initializing connections");
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized connections");

        try {
            this.initializeHDFS();
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + ": Error while initializing Hadoop");
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized Hadoop");

        ExecutorService executorService = Executors.newFixedThreadPool(this.getFileSocketMap().size());
        try {
            int i = getFileSocketMap().size() - 1;

            for (List<String> f : this.getFileSocketMap().keySet()) {
                executorService.submit(new SocketHandler(this, f, i, ProgramPhase.INIT));
                i--;
            }
            executorService.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized SocketHandlers");
    }
    /**
     * The manageEnd method manages the end of the program.
     * If the error is present, the program is aborted.
     * Otherwise the files are merged and the program is ended.
     * @param identifier represents the identifier of the worker which has finished.
     * @throws IOException if an error occurs while managing the end of the program.
     */
    public synchronized void manageEnd(int identifier) throws IOException {
        if (errorPresent) {
            logger.error(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
            System.out.println(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
            return;
        }
        endedWorkers++;
        if (endedWorkers == Math.min(addresses.size(), files.size())) {
            hadoopCoordinator.mergeFiles(outputId, programId, changeKey && reduce);
            hadoopCoordinator.deleteFiles(programId, changeKey && reduce);
            hadoopCoordinator.closeFileSystem();
        }
    }


}
