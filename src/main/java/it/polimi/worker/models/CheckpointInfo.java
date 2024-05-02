package it.polimi.worker.models;

import it.polimi.common.KeyValuePair;

/**
 * The CheckpointInfo class is a record used to store the information about the checkpoint.
 * It contains the count of the checkpoint, the end flag, the remaining string and the key value pair.
 *
 * @param count           represents the count of the checkpoint.
 * @param end             represents the end flag.
 * @param remainingString represents the remaining string.
 * @param keyValuePair    represents the key value pair.
 * @see KeyValuePair
 */
public record CheckpointInfo(int count, boolean end, String remainingString, KeyValuePair keyValuePair) {

}
