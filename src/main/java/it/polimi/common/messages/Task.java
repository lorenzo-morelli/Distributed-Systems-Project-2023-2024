package it.polimi.common.messages;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

// Class representing a task to be executed by a worker
public class Task implements Serializable{
    private final List<MutablePair<String, String>> operators;
    private final String pathFile;
    private final boolean isPresentStep2;
    private final Integer taskId;
    private final Integer id;
    public Task(Integer id, List<MutablePair<String, String>> list, String pathFile,boolean isPresentStep2,Integer taskId) {
        this.operators = list;
        this.pathFile = pathFile;
        this.isPresentStep2 = isPresentStep2;
        this.taskId = taskId;
        this.id = id;
    }
    public List<MutablePair<String, String>> getOperators(){
        return operators;
    }
    public String getPathFile(){
        return pathFile;
    }
    public boolean isPresentStep2(){
        return isPresentStep2;
    }
    public Integer getTaskId(){
        return taskId;
    }
    public Integer getId(){
        return id;
    }   

}