package it.polimi.common.messages;


import org.apache.commons.lang3.tuple.MutablePair;

public class ReduceOperation extends Operation {
    private final MutablePair<String, String> reduce;
    private final MutablePair<Integer, Integer> keys;

    public ReduceOperation(String programId, MutablePair<String, String> reduce, MutablePair<Integer, Integer> keys, Integer identifier) {
        super(programId, identifier); // Call to superclass constructor
        this.reduce = reduce;
        this.keys = keys;
    }

    public MutablePair<String, String> getReduce() {
        return reduce;
    }

    public MutablePair<Integer, Integer> getKeys() {
        return keys;
    }
}
