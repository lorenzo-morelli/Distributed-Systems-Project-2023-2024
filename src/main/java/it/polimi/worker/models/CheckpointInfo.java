package it.polimi.worker.models;

import it.polimi.common.KeyValuePair;

public record CheckpointInfo(int count, boolean end, String remainingString, KeyValuePair keyValuePair) {

}
