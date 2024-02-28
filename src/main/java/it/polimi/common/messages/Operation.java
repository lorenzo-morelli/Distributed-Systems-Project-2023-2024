package it.polimi.common.messages;

import java.io.Serializable;

public abstract class Operation implements Serializable{
    private final String programId;
    private final Integer identifier;
    public Operation(String programId, Integer identifier) {
        this.programId = programId;
        this.identifier = identifier;
    }
    public String getProgramId(){
        return programId;
    }
    public Integer getIdentifier(){
        return identifier;
    }
}
