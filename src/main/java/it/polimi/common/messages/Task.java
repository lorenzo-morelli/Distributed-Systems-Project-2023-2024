package it.polimi.common.Messages;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.KeyValuePair;

// Class representing a task to be executed by a worker
public class Task implements Serializable{
    List<MutablePair<String, String>> operators; // e.g., "map", "filter", "changeKey", "reduce"
    List<KeyValuePair> data;
    public Task(List<MutablePair<String, String>> list, List<KeyValuePair> data) {
        this.operators = list;
        this.data = data;
    }
    public List<MutablePair<String, String>> getOperators(){
        return operators;
    }
    public List<KeyValuePair> getData(){
        return data;
    }
}