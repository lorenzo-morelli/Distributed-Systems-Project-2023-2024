package it.polimi.common.messages;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

// Class representing a task to be executed by a worker
public class Task implements Serializable{
    private List<MutablePair<String, String>> operators; // e.g., "map", "filter", "changeKey", "reduce"
    private String pathFile;
    public Task(List<MutablePair<String, String>> list, String pathFile) {
        this.operators = list;
        this.pathFile = pathFile;
    }
    public List<MutablePair<String, String>> getOperators(){
        return operators;
    }
    public String getPathFile(){
        return pathFile;
    }
}