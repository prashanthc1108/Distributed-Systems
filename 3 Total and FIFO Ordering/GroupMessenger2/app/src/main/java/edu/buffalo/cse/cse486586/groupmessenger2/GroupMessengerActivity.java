package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.app.Notification;
import android.app.Notification.Action;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.PriorityBlockingQueue;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    boolean DOFIFOORDERING = false;

    private final Object DBLock = new Object();
    private final Object mQLock = new Object();
    private final Object TOSeqLock = new Object();

    static final String REMOTE_PORTS[] = {"11108","11112","11116","11120","11124"};
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String DELIM = " ";

    public static int[] FIFISEQ = new int[5];
    public static HashMap<Integer, MyMulticastMessage> FIFOBuffer0 = new HashMap<Integer, MyMulticastMessage>();
    public static HashMap<Integer, MyMulticastMessage> FIFOBuffer1 = new HashMap<Integer, MyMulticastMessage>();
    public static HashMap<Integer, MyMulticastMessage> FIFOBuffer2 = new HashMap<Integer, MyMulticastMessage>();
    public static HashMap<Integer, MyMulticastMessage> FIFOBuffer3 = new HashMap<Integer, MyMulticastMessage>();
    public static HashMap<Integer, MyMulticastMessage> FIFOBuffer4 = new HashMap<Integer, MyMulticastMessage>();


//    public static int TOTALORDERSEQ = 0;
    public static float TOTALORDERSEQ = 0;
    public static int PID;
    public static boolean[] ACTIVEPROC = {true, true, true, true, true};

    public static int OMSG = 0;
    public static int MYSEQ = 1;
    public static int FINSEQ = 2;
    public static int ACK = 3;
    public static int PROCSTAT = 4;

    public static int dbKey = 0;

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final int CLEANUPINTERVAL = 2000;

    public static PriorityBlockingQueue msgQ = new PriorityBlockingQueue();

//    public static String formMsgString(int msgType, String msg, int totalOrder, int pid) {
    public static String formMsgString(int msgType, String msg, float totalOrder, int pid) {

        return(pid+"_"+FIFISEQ[pid]+DELIM+msgType+DELIM+pid+DELIM+totalOrder+DELIM+FIFISEQ[pid]+DELIM+msg+DELIM+System.currentTimeMillis());

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
//my changes
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        PID = (Integer.parseInt(myPort)%11108)/4;
//        TOTALORDERSEQ = Float.parseFloat("0."+PID);
        Log.e(TAG, "My Port:"+myPort+", My PID:"+PID+", init TOSeq:"+TOTALORDERSEQ);

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        //changes end


        Thread cleanupThread = new Thread(new CleanupThread(CLEANUPINTERVAL));
        cleanupThread.start();

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                TextView tv1=(TextView) findViewById(R.id.textView1);
                tv1.setMovementMethod(new ScrollingMovementMethod());
                String msg = editText.getText().toString() ;
                tv1.append("Tx:["+msg+"]\n");
                editText.setText(""); // This is one way to reset the input box.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {



        protected void onProgressUpdate(String... strings) {
                /*
                 * The following code displays what is received in doInBackground().
                 */
            TextView tv1 = (TextView) findViewById(R.id.textView1);
            tv1.setMovementMethod(new ScrollingMovementMethod());
            tv1.append("Rx:[" + strings[0].trim() + "]\n");
            return;

        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            int msgKey=0;


            while (TRUE) {
                try {
                    Socket cliSock = serverSocket.accept();
                    String str;

                    BufferedReader br = new BufferedReader(new InputStreamReader(cliSock.getInputStream()));
                    do {
                        str = br.readLine();
                    } while (str == null);


//                    writetoDB(str);

                    int incomingMsgType = Integer.parseInt(str.split(DELIM)[1]);
                    String msgId = str.split(DELIM)[0];
//                    int newTOSeq = Integer.parseInt(str.split(DELIM)[3]);
                    float newTOSeq = Float.parseFloat(str.split(DELIM)[3]);
                    boolean shouldUpdate = false;


                    synchronized (TOSeqLock) {
                        if (incomingMsgType == OMSG) {
                            //get obj, put in queue
                            String newAckStr;
                            TOTALORDERSEQ += 1;
                            newAckStr = formMsgString(MYSEQ, str.split(DELIM)[5], TOTALORDERSEQ,Integer.parseInt(str.split(DELIM)[2]));

                            MyMulticastMessage msg = new MyMulticastMessage(str);
                            synchronized (mQLock) {
                                msgQ.add(msg);
                            }
                            Log.e(TAG,"Added msg to Q:"+msg+" proposed:"+TOTALORDERSEQ);
                            publishProgress(msg.multicastMsg);

                            PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
                            ackSender.println(newAckStr);
                            shouldUpdate = false;

                        } else if (incomingMsgType == FINSEQ) {
                            //send ack, update Q in a thread

//                            if (newTOSeq > TOTALORDERSEQ) {
//                                TOTALORDERSEQ = newTOSeq;
//                            }
                            if (Math.floor(newTOSeq) > TOTALORDERSEQ) {
                                TOTALORDERSEQ = (float)Math.floor((newTOSeq));
                            }
//                        float totalSeq = TOTALORDERSEQ;

//                            String newAckStr = formMsgString(ACK, str.split(DELIM)[5], newTOSeq,Integer.parseInt(str.split(DELIM)[2]));
                            String newAckStr = formMsgString(ACK, str.split(DELIM)[5], newTOSeq,Integer.parseInt(str.split(DELIM)[2]));
                            PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
                            ackSender.println(newAckStr);
//                        publishProgress(str+"Sending:["+newAckStr+"]..will update Q");
                            Log.e(TAG,"Got final Seq:"+newTOSeq+" Need to update msg:"+msgId);
                            shouldUpdate = true;

                        } else if(incomingMsgType == PROCSTAT){
                            int deadProcID = Integer.parseInt(str.split(DELIM)[2]);
                            ACTIVEPROC[deadProcID] = false;
                            Log.e(TAG,"Got deadProcID:"+deadProcID);
//                            String newAckStr = formMsgString(ACK, "done", newTOSeq,Integer.parseInt(str.split(DELIM)[2]));
                            String newAckStr = formMsgString(ACK, "done", newTOSeq,Integer.parseInt(str.split(DELIM)[2]));
                            PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
                            ackSender.println(newAckStr);
                            Thread cleanupThreadImm = new Thread(new CleanupThreadImmediate(deadProcID));
                            cleanupThreadImm.start();
                            //Trigger cleanup thread
                        }

                    }
                    if(shouldUpdate) {
//                        Thread updateQThread = new Thread(new ProcessPriorityQ(msgId, newTOSeq));
                        Thread updateQThread = new Thread(new ProcessPriorityQ(msgId, newTOSeq));
                        updateQThread.start();
                        shouldUpdate = false;
                    }
                    br.close();
                    cliSock.close();
                } catch (IOException e) {
                    Log.e(TAG,"Error when reading and publishingProgress");
                    e.printStackTrace();
                }
//                Log.e(TAG,"Inserted Messages:");
//                int i =0;
//                while(!msgQ.isEmpty()) {
//                    Log.e(TAG, "Msg no " + (i + 1) + ": " + msgQ.poll());
//
//                }

            }


            return null;
        }


    }




    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
//            int proposedSeq;
            float proposedSeq;
//            int finSeq;
            float finSeq;
            synchronized (TOSeqLock) {
                proposedSeq = TOTALORDERSEQ;
                finSeq = TOTALORDERSEQ;
            }
            msgToSend = msgToSend.replace("\n", "");

            String newMsg = formMsgString(OMSG,msgToSend,finSeq, PID);
            Log.e(TAG,"Sending new Msg: "+newMsg);
            boolean deadProc = false;

            for(int i=0;i<5;i++){
                if(ACTIVEPROC[i]) {
                    proposedSeq = sendMessageToRemote(REMOTE_PORTS[i], newMsg, OMSG);
                    proposedSeq = proposedSeq+((float)i/10);
                    if(proposedSeq < 0){
                        ACTIVEPROC[i]=false;
                        Log.e(TAG,"Process ["+i+"] died when sending msg, will inform everyone");
                        deadProc = true;
                    }else if (proposedSeq>finSeq){
                        finSeq = proposedSeq;
                    }
                }
            }


            if(deadProc){
                publishDeadProc(finSeq);
                deadProc = false;

            }
//            printActivenodes();
            synchronized (TOSeqLock) {
                if(Math.floor(finSeq)>TOTALORDERSEQ) {
                    TOTALORDERSEQ = (float)Math.floor(finSeq);
                }
//                TOTALORDERSEQ = finSeq;
            }
            Log.e(TAG,"Sending Final TOSeq:"+finSeq+" for:"+PID+"_"+FIFISEQ[PID]);

            newMsg = formMsgString(FINSEQ,msgToSend,finSeq, PID);
            for(int i=0;i<5;i++){
                if(ACTIVEPROC[i]) {
                    proposedSeq = sendMessageToRemote(REMOTE_PORTS[i], newMsg, FINSEQ);
                    if(proposedSeq == -1){
                        Log.e(TAG,"Process ["+i+"] died when sending finSeq, will inform everyone");
                        ACTIVEPROC[i]=false;
                        deadProc = true;
                    }
                }
            }


            if(deadProc){
                publishDeadProc(finSeq);
                deadProc = false;

            }


            FIFISEQ[PID]++;

            return null;
        }

//        public void publishDeadProc(int finSeq) {
        public void publishDeadProc(float finSeq) {

            String newMsg;
            for (int i = 0; i < 5; i++) {
                if (ACTIVEPROC[i]) {
                    for (int j = 0; j < 5; j++) {
                        if (!ACTIVEPROC[j]) {
                            newMsg = PID + "_" + FIFISEQ[PID] + DELIM + PROCSTAT + DELIM + j + DELIM + finSeq;
//                            int ret = sendMessageToRemote(REMOTE_PORTS[i], newMsg, PROCSTAT);
                            float ret = sendMessageToRemote(REMOTE_PORTS[i], newMsg, PROCSTAT);
                            if(ret == -1){
                                ACTIVEPROC[i]= false;
                                Log.e(TAG,"Process ["+i+"] died when informing that other process died, will inform everyone");

                                publishDeadProc(finSeq);
                            }
                        }
                    }

                }
            }
        }

        private void printActivenodes(){
            Log.e(TAG,"Active nodes are:");
            for (int i =0; i<5; i++){
                if(ACTIVEPROC[i]) {
                    Log.e(TAG, i+",");
                }
            }
        }



        private String printMsg(String msg){
            String[] msgs = msg.split(DELIM);
            return ("MsgID:"+msgs[0]+"|MsgType:"+msgs[1]+"|PID:"+msgs[2]+"|TOSeq:"+msgs[3]+"|FIFOSeq:"+msgs[4]+"|Msg:"+msgs[5]);
        }

        private String appendTime(String msg){
            long date = System.currentTimeMillis();
            return msg + DELIM + date;

        }


//        private int sendMessageToRemote(String remotePort, String msgToSend, int msgType) {
        private float sendMessageToRemote(String remotePort, String msgToSend, int msgType) {
//            int proposedSeq=-1;
            float proposedSeq=-1;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                socket.setSoTimeout(500);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

//                msgToSend = appendTime(msgToSend);

                out.println(msgToSend);
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String reply;
                long t1 = System.currentTimeMillis();
                do {
                    reply = br.readLine();
//                    Log.e(TAG, "Waiting.....");
                    //TODO remove below 4 lines and above after adding timeout
                    long t2 = System.currentTimeMillis();
                    if (t2-t1>500){
                        return -1;
                    }
                }while (reply==null);
                if(msgType == OMSG) {
//                    proposedSeq = Integer.parseInt(reply.split(DELIM)[3]);
                    proposedSeq = Float.parseFloat(reply.split(DELIM)[3]);
                }else{
                    proposedSeq = 0;
                }

//                while (br.readLine()==null) ;
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
//            } catch (SocketTimeoutException e){
                Log.e(TAG, "Proc with port "+remotePort+" is down");
                return -1;
            }
            return proposedSeq;
        }
    }

    public class CleanupThread implements Runnable{

        long sleepInterval;
        public CleanupThread(long period){
            sleepInterval = period;
        }
        public void run(){
            while(true){
                synchronized (mQLock) {
                    while (!msgQ.isEmpty()) {
                        MyMulticastMessage tempMsg = (MyMulticastMessage) msgQ.peek();
                        if (ACTIVEPROC[tempMsg.sendingPID]) {
                            break;
                        } else {
                            if(tempMsg.isDeliverable){
                                break;
                            }else {
                                Log.e(TAG, "Clearing msg:" + tempMsg.msgID);
                                msgQ.poll();
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(sleepInterval);
                }catch (Exception e){
                    Log.e(TAG,"In Cleanup Thread, sleep exception");
                }
            }
        }

    }

    public class CleanupThreadImmediate implements Runnable{

        int deadProc;
        public CleanupThreadImmediate(int pid){
            this.deadProc = pid;
        }
        public void run(){
            int count =0;
                synchronized (mQLock) {
                    PriorityBlockingQueue tempQ = new PriorityBlockingQueue();
                    MyMulticastMessage tempMsg;
                    while (!msgQ.isEmpty()){
                        count++;
                        tempMsg = (MyMulticastMessage)msgQ.poll();
                        if(tempMsg.sendingPID == this.deadProc){

                            Log.e(TAG,"Removed Msg: "+tempMsg.msgID+" as process just died");
                            continue;
                        }else{
                              tempQ.add(tempMsg);
                        }
                    }
                    msgQ.clear();
                    msgQ = tempQ;
                }
        }

    }

    public class ProcessPriorityQ implements Runnable{


        private ContentResolver mContentResolver = getContentResolver();
        private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
        private ContentValues mContentValues =  new ContentValues();


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        String msgID;
//        int finTOSeq;
        float finTOSeq;

//        public ProcessPriorityQ(String id, int seq){
        public ProcessPriorityQ(String id, float seq){
            msgID = id;
            finTOSeq = seq;
        }

        public int updateMsgQ(){
            int ret =0, count =0;
            PriorityBlockingQueue tempQ = new PriorityBlockingQueue();
            MyMulticastMessage tempMsg;
            while (!msgQ.isEmpty()){
                count++;
                tempMsg = (MyMulticastMessage)msgQ.poll();
                if(tempMsg.isDeliverable){
                    if(ACTIVEPROC[tempMsg.sendingPID]) {
                        tempQ.add(tempMsg);
                    }
                }else{
                    if(msgID.equals(tempMsg.msgID)){
                        if(ACTIVEPROC[tempMsg.sendingPID]) {
                            tempMsg.totalSeqID = finTOSeq;
                            tempMsg.isDeliverable = true;
                            Log.e(TAG,"Setting Total Seq as "+finTOSeq+" for Msg: "+msgID+" and Marking as deliverable. Position:"+(count-1));
                            tempQ.add(tempMsg);
                            ret = 1;
                        }
                    }else{
                        if(ACTIVEPROC[tempMsg.sendingPID]) {
                            tempQ.add(tempMsg);
                        }
                    }
                }
            }
            msgQ.clear();
            msgQ = tempQ;
            return ret;
        }

        public void writeToDB(String msg, String msgID){
            //write to DB
            synchronized (DBLock) {

                mContentValues.put(KEY_FIELD, Integer.toString(dbKey));
                mContentValues.put(VALUE_FIELD, msg);
                try {
                    Log.e(TAG, "Inserting "+msgID+" into DB: key[" + dbKey + "] val[" + msg + "]");
                    mContentResolver.insert(mUri, mContentValues);
                    dbKey++;
                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                    return;
                }
//                        publishProgress(str);
                return;
            }
        }

        public HashMap<Integer,MyMulticastMessage> getFIFOHASH(int pid){
            if(pid == 0){
                return FIFOBuffer0;
            }else if(pid == 1){
                return FIFOBuffer1;
            }else if(pid == 2){
                return FIFOBuffer2;
            }else if(pid == 3){
                return FIFOBuffer3;
            }else if(pid == 4){
                return FIFOBuffer4;
            }
            return null;
        }
        public void setFIFOHASH(int pid, HashMap<Integer, MyMulticastMessage> fifobuff){
            if(pid == 0){
                FIFOBuffer0.clear();
                FIFOBuffer0 = fifobuff;
            }else if(pid == 1){
                FIFOBuffer1.clear();
                FIFOBuffer1 = fifobuff;
            }else if(pid == 2){
                FIFOBuffer2.clear();
                FIFOBuffer2 = fifobuff;
            }else if(pid == 3){
                FIFOBuffer3.clear();
                FIFOBuffer3 = fifobuff;
            }else if(pid == 4){
                FIFOBuffer4.clear();
                FIFOBuffer4 = fifobuff;
            }
        }





        public void fifoInsert(MyMulticastMessage msg){
            int sendersPID = msg.sendingPID;
            int msgOrder = msg.FIFOSeqID;
            HashMap<Integer,MyMulticastMessage> fifoBuff = getFIFOHASH(sendersPID);
            if(msgOrder == FIFISEQ[sendersPID]+1){
                writeToDB(msg.multicastMsg,msg.msgID);
                FIFISEQ[sendersPID]=msgOrder;
                Log.e(TAG,"sender's PID: "+sendersPID);
                if(!(fifoBuff.isEmpty())) {
                    if (fifoBuff.containsKey(msgOrder + 1)) {
                        fifoInsert(fifoBuff.get(msgOrder + 1));
                    }
                }
            }else if (msg.FIFOSeqID < FIFISEQ[msg.sendingPID]+1){
                return;
            }else{
                fifoBuff.put(msg.FIFOSeqID,msg);
            }
            setFIFOHASH(sendersPID,fifoBuff);
        }

        public void popFromQ(){
            while(!msgQ.isEmpty()){
                MyMulticastMessage tempMsg = (MyMulticastMessage) msgQ.peek();
                Log.e(TAG,"Checking if deliverable head:"+tempMsg.msgID+" seq:"+tempMsg.totalSeqID+" sentTime:"+tempMsg.msgGenTime);
                if (tempMsg.isDeliverable){
                    tempMsg = (MyMulticastMessage) msgQ.poll();
                    Log.e(TAG,"deliverable head:"+tempMsg.msgID+" seq:"+tempMsg.totalSeqID+" sentTime:"+tempMsg.msgGenTime);

                    if(DOFIFOORDERING){
                        fifoInsert(tempMsg);
                    }else {
                            writeToDB(tempMsg.multicastMsg,tempMsg.msgID);

                    }
                }else{
                    return;
                }
            }
        }

        public void run(){
            synchronized (mQLock) {
                updateMsgQ();

                popFromQ();
            }
        }
    }




}
