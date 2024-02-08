package it.polimi.coordinator;

import java.util.*;

public class KeyAssignmentManager {

    private Map<SocketHandler, List<Integer>> currentAssignments;
    public KeyAssignmentManager() {
        currentAssignments = new HashMap<>();
    }

    public void insertAssignment(SocketHandler worker, List<Integer> keys, Integer num) {
        currentAssignments.put(worker, keys);
        try{
        if (currentAssignments.size() == num) {
            assignKeys(determineNewAssignmentsWithLoadBalancing());
        }}catch(Exception e){
            e.printStackTrace();
        }
    }

    // Method to determine new worker assignments with load balancing for selected keys
    public Map<SocketHandler, List<Integer>> determineNewAssignmentsWithLoadBalancing() {
        Map<SocketHandler, List<Integer>> newAssignments = new HashMap<>();
        for(SocketHandler s: currentAssignments.keySet()){
            newAssignments.put(s,new ArrayList<>());
        }
        Set<Integer> assignedKeys = new HashSet<>();

        for (Map.Entry<SocketHandler, List<Integer>> entry : currentAssignments.entrySet()) {

            List<Integer> keys = entry.getValue();

            for (Integer key : keys) { 
                if(!assignedKeys.contains(key)){
                    // Load balance keys with more than one worker
                    SocketHandler newWorkerSocket = getLeastLoadedWorker(newAssignments);
                    newAssignments.get(newWorkerSocket).add(key);
                    assignedKeys.add(key);
                }
            }
        }
        return newAssignments;
    }

    private SocketHandler getLeastLoadedWorker(Map<SocketHandler, List<Integer>> newAssignments) {
        Optional<Map.Entry<SocketHandler, List<Integer>>> leastLoadedWorker = newAssignments.entrySet().stream()
                .min(Comparator.comparingInt(entry -> entry.getValue().size()));
        return leastLoadedWorker.map(Map.Entry::getKey).orElseThrow();
    }

    private void assignKeys(Map<SocketHandler, List<Integer>> newAssignments) {
        for (SocketHandler s : newAssignments.keySet()) {
            s.sendNewAssignment(newAssignments.get(s));
        }
    }
}
