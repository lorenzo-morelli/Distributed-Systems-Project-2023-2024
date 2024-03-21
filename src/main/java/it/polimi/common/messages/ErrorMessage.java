package it.polimi.common.messages;

import java.io.Serializable;

/**
 * The Error message class is a message used to notify the error to the coordinator node.
 * It contains the error message. 
 */

public record ErrorMessage(String message) implements Serializable {
}
