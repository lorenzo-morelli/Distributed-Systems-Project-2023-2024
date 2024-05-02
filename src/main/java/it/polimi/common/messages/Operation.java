package it.polimi.common.messages;

import java.io.Serializable;

/**
 * The Operation class is an abstract class used to notify the operations to the worker node.
 * It contains the program id and the identifier of the worker for a specific program.
 * It is extended by the NormalOperations and ReduceOperation classes.
 */
public abstract class Operation implements Serializable {
    private final String programId;
    private final int identifier;

    /**
     * The constructor creates a new Operation.
     *
     * @param programId  represents the program id.
     * @param identifier represents the identifier of the worker for a specific program.
     */
    public Operation(String programId, int identifier) {
        this.programId = programId;
        this.identifier = identifier;
    }

    /**
     * The getProgramId method returns the program id.
     *
     * @return the program id.
     */
    public String getProgramId() {
        return programId;
    }

    /**
     * The getIdentifier method returns the identifier of the worker for a specific program.
     *
     * @return the identifier of the worker for a specific program.
     */
    public int getIdentifier() {
        return identifier;
    }
}
