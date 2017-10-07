package edu.buffalo.cse.cse486586.groupmessenger2;

import java.util.regex.Pattern;

import static edu.buffalo.cse.cse486586.groupmessenger2.GroupMessengerActivity.DELIM;
import static edu.buffalo.cse.cse486586.groupmessenger2.GroupMessengerActivity.PID;

/**
 * Created by prashanth on 3/4/17.
 */

public class MyMulticastMessage implements Comparable<MyMulticastMessage>{
    public String msgID;
    public int sendingPID;
//    public int totalSeqID;
    public float totalSeqID;
    public int FIFOSeqID;
    public String multicastMsg;
    public boolean isDeliverable;
    public long msgGenTime;

    public MyMulticastMessage(String rxMsg){
        //parse msg and store in appropriate fields

        String msgFields[] = rxMsg.split(DELIM);
//        String msgFields[] = Pattern.compile(DELIM).split(rxMsg, -1);
        sendingPID = Integer.parseInt(msgFields[2]);
//        totalSeqID = Integer.parseInt(msgFields[3]);
        totalSeqID = Float.parseFloat(msgFields[3]);
        FIFOSeqID = Integer.parseInt(msgFields[4]);
        multicastMsg = msgFields[5];
        msgGenTime = Long.parseLong(msgFields[6]);
        msgID = msgFields[0];
        isDeliverable = false;

    }

    public String toString(){
        return "{MsgID:"+this.msgID+"|PID:"+this.sendingPID+"|TOSeq:"+this.totalSeqID+"|FIFOSeq:"+this.FIFOSeqID+
                "|Msg:"+this.multicastMsg+"|isDeliverable:"+this.isDeliverable+"|MsgGenAt:"+this.msgGenTime+"}";
    }

//    public String toString(){
//        return this.msgID+DELIM+this.sendingPID+DELIM+this.sendingPID+DELIM+this.totalSeqID+DELIM+this.FIFOSeqID+
//                DELIM+this.multicastMsg+DELIM+this.msgGenTime;
//    }

    public int compareTo(MyMulticastMessage secondMsg){
        if(this.totalSeqID>secondMsg.totalSeqID){
            return 1;
        }else if (this.totalSeqID<secondMsg.totalSeqID){
            return -1;
        }else{
//            if(this.msgGenTime>secondMsg.msgGenTime){
//                return 1;
//            }else if(this.msgGenTime<secondMsg.msgGenTime){
//                return -1;
//            }else {
                return 0;
//            }
        }
    }
}