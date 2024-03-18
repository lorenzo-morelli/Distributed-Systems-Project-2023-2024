package it.polimi.worker.utils;

public class CheckpointInfo {
    private final boolean end;
    private final int count;
    private final String remainingString;
    public CheckpointInfo(int count, boolean end, String remainingString){
        this.count = count;
        this.end = end;
        this.remainingString = remainingString;
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

}
