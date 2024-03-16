package it.polimi.worker.utils;

public class Data{
    
    private final boolean end;
    private final String data;
    
    public Data(String data, boolean end){
        this.data = data;
        this.end = end;
    }
    
    public boolean getEnd(){
        return end;
    }
    
    public String getData(){
        return data;
    }
}
