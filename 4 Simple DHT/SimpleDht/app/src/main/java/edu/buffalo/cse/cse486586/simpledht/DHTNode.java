package edu.buffalo.cse.cse486586.simpledht;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider.DELIM;

/**
 * Created by prashanth on 4/11/17.
 */

public class DHTNode {
    public String port = "";
    public String portID = "";
    public String successorPort = "";
    public String successorId = "";
    public String predecessorPort = "";
    public String predecessorId = "";
    public boolean active = false;

    public DHTNode(){
        active = false;
        port = portID = successorPort = successorId = predecessorPort = predecessorId = "";
    }

    public void addNode(String str){
        String[] nodeInfo = str.split(DELIM);
        this.port = nodeInfo[1];
        this.portID = nodeInfo[1];
        this.successorPort = nodeInfo[1];
        this.successorId = nodeInfo[1];
        this.predecessorPort = nodeInfo[1];
        this.predecessorId = nodeInfo[1];
        this.active = true;
    }

    public String toString(){
        return "[port ="+this.port+" portID ="+this.portID+" successorPort ="+this.successorPort+" successorId ="+this.successorId+
                " predecessorPort ="+this.predecessorPort+" predecessorId ="+this.predecessorId+" status ="+this.active+" ]";
    }
}
