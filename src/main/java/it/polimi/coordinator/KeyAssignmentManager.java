package it.polimi.coordinator;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The KeyAssignmentManager class is responsible for managing the key assignments to workers.
 * It contains methods to insert assignments and determine new assignments with load balancing.
 */
public class KeyAssignmentManager {

    private final Map<SocketHandler, MutablePair<Integer, Integer>> assignments;
    private volatile Boolean canProceed;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private final HadoopCoordinator hadoopCoordinator;
    private final String programId;
    /**
     * KeyAssignmentManager class constructor
     * @param hadoopCoordinator it is the hadoop coordinator.
     * @param programId it is the program id.
     */
    public KeyAssignmentManager(HadoopCoordinator hadoopCoordinator, String programId) {
        assignments = new HashMap<>();
        canProceed = false;
        this.hadoopCoordinator = hadoopCoordinator;
        this.programId = programId;
    }
    /**
     * The getAssignments method returns the assignments.
     * @return the assignments.
     */
    public Map<SocketHandler, MutablePair<Integer, Integer>> getAssignments() {
        return assignments;
    }
    /**
     * The canProceed method returns the canProceed flag which indicates if the assignments have been computed.
     * @return the canProceed flag.
     */
    public Boolean canProceed() {
        return canProceed;
    }
    /**
     * The insertAssignment method inserts the worker keys.
     * @param worker represents the worker.
     * @param num represents the number of workers.
     * @throws IOException if it is not possible to insert the worker keys or determine the new assignments with load balancing.
     */
    public synchronized void insertAssignment(SocketHandler worker, int num) throws IOException {
        logger.info(Thread.currentThread().getName() + ": Inserting worker keys");
        assignments.put(worker, null);
        if (assignments.size() == num) {
            determineNewAssignmentsWithLoadBalancing();
        }
    }
    /**
     * The determineNewAssignmentsWithLoadBalancing method determines the new worker assignments with load balancing and sets the canProceed flag to true.
     * @throws IOException if it is not possible to determine the new worker assignments with load balancing.
     */
    public void determineNewAssignmentsWithLoadBalancing() throws IOException {
        logger.info(Thread.currentThread().getName() + ": Determining new worker assignments with load balancing");

        int numWorkers = assignments.size();


        int keysSize = hadoopCoordinator.getKeysSize(programId);

        int keysPerWorker = keysSize / numWorkers;
        int remainingKeys = keysSize % numWorkers;

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
