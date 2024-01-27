package it.polimi.coordinator;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;

import it.polimi.common.Address;
import it.polimi.common.KeyValuePair;
import it.polimi.common.Operator;

public class Coordinator {

    private List<List<KeyValuePair>> dataSplitted;
    private List<Operator> operators;
    private List<Socket> clienSockets;

    public Coordinator()  {
    }

    public void init(List<Address> addresses, MutablePair<Integer, List<MutablePair<String, String>>> mutablePair, List<KeyValuePair> data) throws Exception{
        this.dataSplitted = splitData(data,mutablePair.getLeft());
        this.operators = handleOperators(mutablePair.getRight()); 
        this.clienSockets = new ArrayList<>();
        initializeAddresses(addresses);
    }


    private void initializeAddresses(List<Address> list) throws Exception{
        for(Address a: list){
            try {
                Socket clientSocket = new Socket(a.getHostname(), a.getPort());
                clienSockets.add(clientSocket);
            } catch (IOException e) {
                throw new Exception("Not possible to initialize the connections with the workers!");
            }
        }
    }

    private List<Operator> handleOperators(List<MutablePair<String,String>> dataFunctions){
        List<Operator> operators = new ArrayList<>();
        for (MutablePair<String,String> df: dataFunctions) {
            String op = df.getLeft(); 
            String fun = df.getRight();
            operators.add(CreateOperator.createOperator(op, fun));
        }
        return operators;
    }
    
    private List<List<KeyValuePair>> splitData(List<KeyValuePair> data, int numberPartitions) {
        List<List<KeyValuePair>> result = new ArrayList<>();
        int dataSize = data.size();
        
        int itemsPerPartition = dataSize / numberPartitions;
        int remainder = dataSize % numberPartitions;

        int startIndex = 0;
        for (int i = 0; i < numberPartitions; i++) {
            int partitionSize = itemsPerPartition + (i < remainder ? 1 : 0);
            int endIndex = startIndex + partitionSize;

            result.add(new ArrayList<>(data.subList(startIndex, endIndex)));
            startIndex = endIndex;
        }

        return result;
    }
    
}
