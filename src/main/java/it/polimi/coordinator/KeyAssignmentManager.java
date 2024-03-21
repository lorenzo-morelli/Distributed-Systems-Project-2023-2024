package it.polimi.coordinator;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KeyAssignmentManager {

    private final Map<SocketHandler, MutablePair<Integer, Integer>> assignments;
    private volatile Boolean canProceed;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private final HadoopCoordinator hadoopCoordinator;
    private final String programId;

    public KeyAssignmentManager(HadoopCoordinator hadoopCoordinator, String programId) {
        assignments = new HashMap<>();
        canProceed = false;
        this.hadoopCoordinator = hadoopCoordinator;
        this.programId = programId;
    }

    public Map<SocketHandler, MutablePair<Integer, Integer>> getAssignments() {
        return assignments;
    }

    public Boolean canProceed() {
        return canProceed;
    }

    public synchronized void insertAssignment(SocketHandler worker, int num) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Inserting worker keys");
        assignments.put(worker, null);
        if (assignments.size() == num) {
            determineNewAssignmentsWithLoadBalancing();
        }
    }

    // Method to determine new worker assignments with load balancing for selected keys
    public void determineNewAssignmentsWithLoadBalancing() throws IOException {
        logger.info(Thread.currentThread().getName() + ": Determining new worker assignments with load balancing");

        // Get the number of workers
        int numWorkers = assignments.size();


        // Get the number of keys
        int keysSize = hadoopCoordinator.getKeysSize(programId);

        // Get the number of keys per worker
        int keysPerWorker = keysSize / numWorkers;
        int remainingKeys = keysSize % numWorkers;

        // Assign keys to workers
        int start = 0;
        int end;
        for (SocketHandler worker : assignments.keySet()) {
            end = start + keysPerWorker;
            if (remainingKeys > 0) {
                end++;
                remainingKeys--;
            }
            assignments.put(worker, new MutablePair<>(start, end));
            start = end;
        }
        canProceed = true;

        logger.info(Thread.currentThread().getName() + ": New worker assignments with load balancing determined");
    }

}
