package it.polimi.worker.utils;

import it.polimi.common.KeyValuePair;

public class CheckpointInfo {
    private final boolean end;
    private final int count;
    private final String remainingString;
    private final KeyValuePair keyValuePair;
    public CheckpointInfo(int count, boolean end, String remainingString, KeyValuePair keyValuePair){
        this.count = count;
        this.end = end;
        this.remainingString = remainingString;
        this.keyValuePair = keyValuePair;
    }
    public boolean getEnd(){
        return end;
    }
    public int getCount(){
        return count;
    }
    public String getRemainingString(){
        return remainingString;
    }
    public KeyValuePair getKeyValuePair(){
        return keyValuePair;
    }

}
