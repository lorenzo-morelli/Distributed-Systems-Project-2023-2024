package it.polimi.coordinator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * The KeyAssignmentManager class is responsible for managing the key assignments to workers.
 * It contains methods to insert assignments and determine new assignments with load balancing.
 * It also manages the case in which the results of the computation of the first phase are empty.
 */
public class KeyAssignmentManager {

    private final Map<SocketHandler, MutablePair<Integer, Integer>> assignments;
    private volatile Boolean canProceed;
    private volatile Boolean exit;
    private static final Logger logger = LogManager.getLogger("it.polimi.Coordinator");
    private final HadoopCoordinator hadoopCoordinator;
    private final String programId;
    private final String outputId;

    /**
     * KeyAssignmentManager class constructor
     *
     * @param hadoopCoordinator it is the hadoop coordinator.
     * @param programId         it is the program id.
     * @param outputId          represents the number of the program.
     */
    public KeyAssignmentManager(HadoopCoordinator hadoopCoordinator, String programId, String outputId) {
        assignments = new HashMap<>();
        canProceed = false;
        exit = false;
        this.hadoopCoordinator = hadoopCoordinator;
        this.programId = programId;
        this.outputId = outputId;
    }

    /**
     * The getAssignments method returns the assignments.
     *
     * @return the assignments.
     */
    public Map<SocketHandler, MutablePair<Integer, Integer>> getAssignments() {
        return assignments;
    }

    /**
     * The canProceed method returns the canProceed flag which indicates if the assignments have been computed.
     *
     * @return the canProceed flag.
     */
    public Boolean canProceed() {
        return canProceed;
    }

    /**
     * The exit method returns the exit flag which indicates if the keys size is 0.
     *
     * @return the exit flag.
     */
    public Boolean exit() {
        return exit;
    }

    /**
     * The insertAssignment method inserts the worker keys.
     *
     * @param worker represents the worker.
     * @param num    represents the number of workers.
     * @throws IOException              if it is not possible to insert the worker keys or determine the new assignments with load balancing.
     * @throws IllegalArgumentException if the keys size is 0.
     */
    public synchronized void insertAssignment(SocketHandler worker, int num) throws IOException, IllegalArgumentException {
        logger.info(Thread.currentThread().getName() + ": Inserting worker keys");
        assignments.put(worker, null);
        if (assignments.size() == num) {
            determineNewAssignmentsWithLoadBalancing();
        }
    }

    /**
     * The determineNewAssignmentsWithLoadBalancing method determines the new worker assignments with load balancing and sets the canProceed flag to true.
     * It also manages the case in which the results of the computation of the first phase are empty by deleting the files and setting the exit flag to true.
     *
     * @throws IOException              if it is not possible to determine the new worker assignments with load balancing.
     * @throws IllegalArgumentException if the keys size is 0.
     */
    public void determineNewAssignmentsWithLoadBalancing() throws IOException, IllegalArgumentException {
        logger.info(Thread.currentThread().getName() + ": Determining new worker assignments with load balancing");

        int numWorkers = assignments.size();


        int keysSize = hadoopCoordinator.getKeysSize(programId);
        if (keysSize == 0) {

            new FileOutputStream("result-" + outputId + ".csv").close();
            hadoopCoordinator.deleteFiles(programId, true);
            exit = true;
            throw new IllegalArgumentException("Keys size is 0");
        }
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
