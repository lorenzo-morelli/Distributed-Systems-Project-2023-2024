package it.polimi.common.messages;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

public class LastReduce implements Serializable {
    private final MutablePair<String, String> reduce;
    private final List<Integer> keys;
    private String programId;
    public LastReduce(String programId,MutablePair<String, String> reduce, List<Integer> keys) {
        this.reduce = reduce;
        this.keys = keys;
        this.programId = programId;
    }
    public MutablePair<String, String> getReduce(){
        return reduce;
    }
    public List<Integer> getKeys(){
        return keys;
    }
    public String getProgramId(){
        return programId;
    }   
}
