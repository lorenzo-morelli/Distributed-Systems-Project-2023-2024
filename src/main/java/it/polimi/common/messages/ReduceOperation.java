package it.polimi.common.messages;


import org.apache.commons.lang3.tuple.MutablePair;

/**
 * The ReduceOperation class is a message used to notify the reduce operation to the worker node.
 * It contains the reduce operation and the keys.
 */
public class ReduceOperation extends Operation {
    private final MutablePair<String, String> reduce;
    private final MutablePair<Integer, Integer> keys;

    /**
     * The constructor creates a new ReduceOperation.
     *
     * @param programId  represents the program id.
     * @param reduce     represents the reduce operation.
     * @param keys       represents the keys interval for which is responsible the worker.
     * @param identifier represents the identifier of the worker for a specific program.
     */
    public ReduceOperation(String programId, MutablePair<String, String> reduce, MutablePair<Integer, Integer> keys, Integer identifier) {
        super(programId, identifier); // Call to superclass constructor
        this.reduce = reduce;
        this.keys = keys;
    }

    /**
     * The getReduce method returns the reduce operation.
     *
     * @return the reduce operation.
     */
    public MutablePair<String, String> getReduce() {
        return reduce;
    }

    /**
     * The getKeys method returns the keys interval for which is responsible the worker.
     *
     * @return the keys interval for which is responsible the worker.
     */
    public MutablePair<Integer, Integer> getKeys() {
        return keys;
    }
}
