package it.polimi.common.messages;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

// Class representing a task to be executed by a worker
public class NormalOperations extends Operation {
    private final List<MutablePair<String, String>> operators;
    private final List<String> pathFiles;
    private final boolean changeKey;
    private final boolean reduce;

    public NormalOperations(String programId, List<MutablePair<String, String>> list, List<String> pathFiles, boolean changeKey, boolean reduce, Integer identifier) {
        super(programId, identifier);
        this.operators = list;
        this.pathFiles = pathFiles;
        this.changeKey = changeKey;
        this.reduce = reduce;
    }

    public List<MutablePair<String, String>> getOperators() {
        return operators;
    }

    public List<String> getPathFiles() {
        return pathFiles;
    }

    public boolean getChangeKey() {
        return changeKey;
    }

    public boolean getReduce() {
        return reduce;
    }
}