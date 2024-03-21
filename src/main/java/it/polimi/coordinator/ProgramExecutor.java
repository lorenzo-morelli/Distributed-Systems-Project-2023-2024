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

    public boolean IsErrorPresent() {
        return errorPresent;
    }

    public void setErrorPresent(boolean errorPresent) {
        this.errorPresent = errorPresent;
    }

    public KeyAssignmentManager getKeyManager() {
        return keyManager;
    }

    public List<Socket> getClientSockets() {
        return clientSockets;
    }

    public List<MutablePair<String, String>> getOperations() {
        return operations;
    }

    public int getNumPartitions() {
        return this.addresses.size();
    }

    public Map<List<String>, Socket> getFileSocketMap() {
        return fileSocketMap;
    }

    public List<Address> getAddresses() {
        return addresses;
    }

    public MutablePair<String, String> getLastReduce() {
        return lastReduce;
    }

    public String getProgramId() {
        return programId;
    }

    public boolean getChangeKey() {
        return changeKey;
    }

    public boolean getReduce() {
        return reduce;
    }

    public int getFilesSize() {
        return files.size();
    }

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
                load.put(a, -1);
            }
        }

        for (Socket s : clientSockets) {
            Address a = new Address(s.getInetAddress().getHostName(), s.getPort());
            if (load.containsKey(a) && !a.hostname().equals(machine)) {
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

    private void initializeHadoop() {
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
            this.initializeHadoop();
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
                executorService.submit(new SocketHandler(this, f, i, CoordinatorPhase.INIT));
                i--;
            }
            executorService.shutdown();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
        logger.info(Thread.currentThread().getName() + " initialized SocketHandlers");
    }

    public synchronized void manageEnd(int identifier) throws IOException {
        if (errorPresent) {
            logger.error(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
            System.out.println(Thread.currentThread().getName() + ": Error present in the program. Aborting...");
            return;
        }
        if (changeKey && reduce) {
            hadoopCoordinator.mergeFiles(outputId, programId, identifier);
            endedWorkers++;
            if (endedWorkers == Math.min(addresses.size(), files.size())) {
                hadoopCoordinator.deleteFiles(programId, changeKey && reduce);
                hadoopCoordinator.closeFileSystem();
            }
        } else {
            endedWorkers++;
            if (endedWorkers == Math.min(addresses.size(), files.size())) {
                hadoopCoordinator.mergeFiles(outputId, programId);
                hadoopCoordinator.deleteFiles(programId, changeKey && reduce);
                hadoopCoordinator.closeFileSystem();
            }
        }
    }


}
