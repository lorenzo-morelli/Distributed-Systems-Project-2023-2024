package it.polimi.common.messages;

import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

// Class representing a task to be executed by a worker
public class NormalOperations extends Operation{
    private final List<MutablePair<String, String>> operators;
    private final List<String> pathFiles;
    private final boolean isPresentStep2;
    private static final long serialVersionUID = 1L;
    public NormalOperations(String programId, List<MutablePair<String, String>> list, List<String> pathFiles,boolean isPresentStep2,Integer identifier) {
        super(programId,identifier); 
        this.operators = list;
        this.pathFiles = pathFiles;
        this.isPresentStep2 = isPresentStep2;
    }
    public List<MutablePair<String, String>> getOperators(){
        return operators;
    }
    public List<String> getPathFiles(){
        return pathFiles;
    }
    public boolean isPresentStep2(){
        return isPresentStep2;
    }
}