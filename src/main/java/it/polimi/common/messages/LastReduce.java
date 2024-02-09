package it.polimi.common.messages;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

public class LastReduce implements Serializable {
    private final MutablePair<String, String> reduce;
    private final List<Integer> keys;
    public LastReduce(MutablePair<String, String> reduce, List<Integer> keys) {
        this.reduce = reduce;
        this.keys = keys;
    }
    public MutablePair<String, String> getReduce(){
        return reduce;
    }
    public List<Integer> getKeys(){
        return keys;
    }

}
