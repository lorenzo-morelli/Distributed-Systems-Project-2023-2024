package it.polimi.coordinator;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KeyAssignmentManager {

    private Map<SocketHandler, MutablePair<Integer,Integer>> currentAssignments;
    private Map<SocketHandler, MutablePair<Integer,Integer>> finalAssignments;
    private volatile Boolean canProceed;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private HadoopCoordinator hadoopCoordinator;
    private String programId;

    public KeyAssignmentManager(HadoopCoordinator hadoopCoordinator, String programId) {
        currentAssignments = new HashMap<>();
        finalAssignments = new HashMap<>();
        canProceed = false;
        this.hadoopCoordinator = hadoopCoordinator;
        this.programId = programId;
    }
    public Map<SocketHandler, MutablePair<Integer,Integer>> getFinalAssignments(){
        return finalAssignments;
    }
    public Boolean canProceed(){
        return canProceed;
    }

    public synchronized void insertAssignment(SocketHandler worker, int num) throws IOException {
        logger.info(Thread.currentThread().getName()+": Inserting worker keys");
        currentAssignments.put(worker, null);
        if (currentAssignments.size() == num) {
            assignKeys(determineNewAssignmentsWithLoadBalancing());
        }
    }

    // Method to determine new worker assignments with load balancing for selected keys
    public Map<SocketHandler, MutablePair<Integer,Integer>> determineNewAssignmentsWithLoadBalancing() throws IOException {
        logger.info(Thread.currentThread().getName()+": Determining new worker assignments with load balancing");
        Map<SocketHandler, MutablePair<Integer,Integer>> newAssignments = new HashMap<>();
        
        // Get the number of workers
        int numWorkers = currentAssignments.size();

        
        // Get the number of keys
        int keysSize = hadoopCoordinator.getKeysSize(programId);
        
        // Get the number of keys per worker
        int keysPerWorker = keysSize / numWorkers;
        int remainingKeys = keysSize % numWorkers;

        // Assign keys to workers
        int start = 0;
        int end = 0;
        for (SocketHandler worker : currentAssignments.keySet()) {
            end = start + keysPerWorker;
            if (remainingKeys > 0) {
                end++;
                remainingKeys--;
            }
            newAssignments.put(worker, new MutablePair<>(start, end));
            start = end;
        }

        logger.info(Thread.currentThread().getName()+": New worker assignments with load balancing determined");
        return newAssignments;
    }


    private void assignKeys(Map<SocketHandler, MutablePair<Integer,Integer>> newAssignments) {
        logger.info(Thread.currentThread().getName()+": Assigning keys to workers");
        canProceed = true;
        finalAssignments = newAssignments;
        logger.info(Thread.currentThread().getName()+": Keys assigned to workers");
    }

    
}
