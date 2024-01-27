package common;

import java.io.Serializable;
import java.util.List;

// Class representing a task to be executed by a worker
public class Task implements Serializable{
    List<Operator> operators; // e.g., "map", "filter", "changeKey", "reduce"
    List<KeyValuePair> data;
    public Task(List<Operator> operators, List<KeyValuePair> data) {
        this.operators = operators;
        this.data = data;
    }
    public List<Operator> getOperators(){
        return operators;
    }
    public List<KeyValuePair> getData(){
        return data;
    }
}