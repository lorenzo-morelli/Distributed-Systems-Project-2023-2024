package it.polimi.worker.utils;

public class CheckpointInfo {
    private final boolean end;
    private final int count;
    public CheckpointInfo(int count, boolean end){
        this.count = count;
        this.end = end;
    }
    public boolean getEnd(){
        return end;
    }
    public int getCount(){
        return count;
    }

}
