package it.polimi.common.messages;

import java.io.Serializable;

public abstract class Operation implements Serializable {
    private final String programId;
    private final int identifier;

    public Operation(String programId, int identifier) {
        this.programId = programId;
        this.identifier = identifier;
    }

    public String getProgramId() {
        return programId;
    }

    public int getIdentifier() {
        return identifier;
    }
}
