package it.polimi.common.messages;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

/**
 * The NormalOperations class is a message used to notify the operations to the worker node.
 * It contains the list of operations, the list of files, the change key flag and the reduce flag.
 */
public class NormalOperations extends Operation {
    private final List<MutablePair<String, String>> operators;
    private final List<String> pathFiles;
    private final boolean changeKey;
    private final boolean reduce;

    /**
     * The constructor creates a new NormalOperations.
     *
     * @param programId  represents the program id.
     * @param list       represents the list of operations.
     * @param pathFiles  represents the list of files on hdfs to process.
     * @param changeKey  represents if the changekey operations is present or not in the list of operations.
     * @param reduce     represents if the reduce operation is present or not in the list of operations.
     * @param identifier represents the identifier of the worker for a specific program.
     */
    public NormalOperations(String programId, List<MutablePair<String, String>> list, List<String> pathFiles, boolean changeKey, boolean reduce, Integer identifier) {
        super(programId, identifier);
        this.operators = list;
        this.pathFiles = pathFiles;
        this.changeKey = changeKey;
        this.reduce = reduce;
    }

    /**
     * The getOperators method returns the list of operations.
     *
     * @return the list of operations.
     */
    public List<MutablePair<String, String>> getOperators() {
        return operators;
    }

    /**
     * The getPathFiles method returns the list of files on hdfs to process.
     *
     * @return the list of files on hdfs to process.
     */
    public List<String> getPathFiles() {
        return pathFiles;
    }

    /**
     * The getChangeKey method returns the change key flag.
     *
     * @return the change key flag to indicate if the change key operation is present or not in the list of operations.
     */
    public boolean getChangeKey() {
        return changeKey;
    }

    /**
     * The getReduce method returns the reduce flag.
     *
     * @return the reduce flag to indicate if the reduce operation is present or not in the list of operations.
     */
    public boolean getReduce() {
        return reduce;
    }
}