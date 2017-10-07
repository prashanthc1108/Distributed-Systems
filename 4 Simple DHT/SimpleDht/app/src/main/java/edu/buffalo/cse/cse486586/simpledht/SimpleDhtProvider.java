package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.widget.TextView;

import static edu.buffalo.cse.cse486586.simpledht.MySQLiteOpenHelper.COLUMN_KEY;
import static edu.buffalo.cse.cse486586.simpledht.MySQLiteOpenHelper.COLUMN_VAL;
import static edu.buffalo.cse.cse486586.simpledht.MySQLiteOpenHelper.TABLE_NAME;
import static java.lang.Boolean.TRUE;

public class SimpleDhtProvider extends ContentProvider {

    MySQLiteOpenHelper mySQLAccess = null;
    SQLiteDatabase sqlDB = null;

    public static final String REMOTE_PORTS[] = {"11108","11112","11116","11120","11124"};
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String DELIM = " ";
    static final String LEADERPORT = "11108";

    public String SUCCESSORID;
    public String PREDECESSORID;
    public String SUCCESSORPORT;
    public String PREDECESSORPORT;
    public String MYID;
    public String MYPORT;
    public String LEADERID;
    public String HEADPORT;
    public String HEADID;
    public String TAILPORT;
    public String TAILID;
    //Maintained only by Leader
    public static DHTNode nodeTable[] = new DHTNode[5];
    public static HashMap<String, Integer> nodePortIndexMap = new HashMap<String, Integer>();
    public static boolean FIRSTNODE = true;
    public static boolean UPDATEFLAG = false;
    public static boolean QUERYRETURNEDFLAG = false;
    public static boolean DELRETURNEDFLAG = false;


    //MSG TYPES:
    public static int ACKSTR = 0;
    public static int JOINREQ = 1;
    public static int UPDATENEIGHBOURS = 2;
    public static int INSERTMSG = 3;
    public static int QUERYMSG = 4;
    public static int DELETEMSG = 5;

    public static Cursor QUERYGLOBALOUTPUT;
    static final String CURSORDELIM = ",";



    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String[] delKeys = {selection};
        sqlDB = mySQLAccess.getWritableDatabase();
        if(selection.equals("@")){
            Log.e(TAG,"Delete all local messages");
            sqlDB.rawQuery("delete * from "+ TABLE_NAME,null);
        }else if(selection.equals("*")){
            Log.e(TAG,"Delete * :all local messages and also delete from others");
            sqlDB.rawQuery("delete * from "+ TABLE_NAME,null);
            forwardKeyToSuccessor(selection,DELETEMSG,"-1",MYPORT);
        }else {
            if(shouldIforward(selection)){
                Log.e(TAG,"Delete single foreign message:["+delKeys+"] forwarding Req");
                forwardKeyToSuccessor(selection,DELETEMSG,"-1",MYPORT);
                while (DELRETURNEDFLAG == false);
                DELRETURNEDFLAG = true;
            }else {
                Log.e(TAG,"Delete single local message:["+delKeys+"]");
                sqlDB.delete(TABLE_NAME, "key = ?", delKeys);
            }
        }
        return 0;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = (String) values.get(COLUMN_KEY);
        String val = (String) values.get(COLUMN_VAL);
        if (shouldIforward(key)) {
            Log.e(TAG,"Key["+key+"] doesn't fit here. Will forward");
            key = key + DELIM + (String) values.get(COLUMN_VAL);
            forwardKeyToSuccessor(key,INSERTMSG,val,MYPORT);
        } else {
            Log.e(TAG,"Inserting Key:"+key+" Val:"+val);
            sqlDB = mySQLAccess.getWritableDatabase();
            sqlDB.insert(TABLE_NAME, null, values);
        }

        return uri;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String [] queryColumnsVals = {selection};
        sqlDB = mySQLAccess.getReadableDatabase();
        Cursor queryCursor;

        if(selection.equals("@")){
            Log.e(TAG,"Query all local messages");
            queryCursor = sqlDB.rawQuery("select * from "+ TABLE_NAME,null);
        }else if(selection.equals("*")){
            Log.e(TAG,"Query all global messages");
            Cursor tempCursor = sqlDB.rawQuery("select * from "+ TABLE_NAME,null);
            Log.e(TAG,"Got all local MSG:["+tempCursor.toString()+"], now forward to others");
            forwardKeyToSuccessor(selection,QUERYMSG,"-1",MYPORT);
            while(QUERYRETURNEDFLAG==false);
            final Cursor mergedCursor = new MergeCursor(new Cursor[] {
                    tempCursor,
                    QUERYGLOBALOUTPUT
            });
            QUERYRETURNEDFLAG = false;
            QUERYGLOBALOUTPUT = null;
            Log.e(TAG,"Apendded local and retrieved cursors from all other AVDs:["+mergedCursor.toString()+"]");
            queryCursor = mergedCursor;
        }else {
            if (shouldIforward(selection)) {
                Log.e(TAG,"Query foreign message with key:["+selection+"]. Forwarding to successor");
                forwardKeyToSuccessor(selection, QUERYMSG, "-1",MYPORT);
                while(QUERYRETURNEDFLAG == false);
                queryCursor = QUERYGLOBALOUTPUT;
                QUERYRETURNEDFLAG = false;
                Log.e(TAG,"Got key, val from other node; Cursor:["+queryCursor.toString()+"]");
            } else {
                String[] queryProjection = {"key", "value"};
                queryCursor = sqlDB.query(TABLE_NAME, queryProjection, "key = ?", queryColumnsVals,
                        null, null, null);
                Log.e(TAG,"Query local message with key:["+selection+"], cursor:["+queryCursor+"]");
            }
        }
        return queryCursor;
    }


    private void forwardKeyToSuccessor(String key, int msgType, String val, String origPort) {
        String msg = msgType+DELIM+SUCCESSORPORT+DELIM+key+DELIM+val+DELIM+origPort;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    private boolean shouldIforward(String key) {
        //TODO check function properly
        try {
            String hashedKey = genHash(key);
            if(MYID.equals(SUCCESSORID)){
                return false;
            }else if(hashedKey.compareTo(PREDECESSORID)>0 && hashedKey.compareTo(MYID)<=0){
                return false;
            }else if(MYID.compareTo(PREDECESSORID)<0 && hashedKey.compareTo(MYID)<=0){
                return false;
            }else if(MYID.compareTo(PREDECESSORID)<0 && hashedKey.compareTo(PREDECESSORID)>0){
                return false;
            }else{
                return true;
            }


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void initNodeTables() {
        for (int i = 0; i<5; i++){
            nodeTable[i] = new DHTNode();
            if(i == 0 ) {
                nodeTable[i].active = true;
                nodeTable[i].successorId = SUCCESSORID;
                nodeTable[i].successorPort = REMOTE_PORTS[i];
                nodeTable[i].port = LEADERPORT;
                nodeTable[i].portID = LEADERID;
                nodeTable[i].predecessorId = PREDECESSORID;
                nodeTable[i].predecessorPort = REMOTE_PORTS[i];
            }else{
                nodeTable[i].active = false;
                nodeTable[i].successorId = "-1";
                nodeTable[i].successorPort = "-1";
                nodeTable[i].port = REMOTE_PORTS[i];
                try {
                    nodeTable[i].portID = genHash((Integer.parseInt(REMOTE_PORTS[i])/2)+"");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                nodeTable[i].predecessorId = "-1";
                nodeTable[i].predecessorPort = "-1";
            }
            HEADID = LEADERID;
            TAILID = HEADID;
            HEADPORT = LEADERPORT;
            TAILPORT = HEADPORT;
            Log.e(TAG,"Inited nodeinfo:"+nodeTable[i]);

        }
    }

    public void printNeighbours(){
        Log.e(TAG,"NEW NEIGHBOURS -- My Port:"+MYPORT+"| My ID:"+MYID+"| Succ port:"+SUCCESSORPORT+"| Succ ID:"
                +SUCCESSORID+"| Pred port:"+PREDECESSORPORT+"| pred id:"+PREDECESSORID);
    }

    public void printNodeStatus(){
        Log.e(TAG,"Printing Node Status:");
        for(int i = 0; i<5; i++) {
            Log.e(TAG, "Node["+(i+1)+"]:"+nodeTable[i]);
        }
    }

    @Override
    public boolean onCreate() {
        mySQLAccess = new MySQLiteOpenHelper(getContext());

        initNeighbours();
        initNodePortIndexHash();
        createServerTask();
        if(MYID.equals(LEADERID)) {
            initNodeTables();
//            printNodeStatus();
            Log.e(TAG,"Waiting for nodes to connect");
        }else{
//            try {
//                Thread.sleep(50);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            joinNW();
            Log.e(TAG,"Joined NW");
            while (UPDATEFLAG == false);
            printNeighbours();
            informSucc();
            informPred();
            UPDATEFLAG = false;
        }

        return false;
    }

    private void initNodePortIndexHash() {
        for(int i = 0; i<5; i++){
            nodePortIndexMap.put(REMOTE_PORTS[i],i);
        }
    }

    private void informPred() {
        String msg = UPDATENEIGHBOURS+DELIM+PREDECESSORPORT+DELIM+MYID+DELIM+MYPORT+DELIM+"1";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    private void informSucc() {
        String msg = UPDATENEIGHBOURS+DELIM+SUCCESSORPORT+DELIM+MYID+DELIM+MYPORT+DELIM+"0";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }


    private void createServerTask() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
    }

    private void joinNW() {
        String msg = JOINREQ+DELIM+MYID;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
    }

    private void initNeighbours() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        MYPORT = (Integer.parseInt(portStr)*2)+"";
        Log.e(TAG,"Myport:"+MYPORT);

        PREDECESSORPORT = MYPORT;
        SUCCESSORPORT = MYPORT;


        try{
            MYID = genHash(Integer.parseInt(MYPORT)/2+"");
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException hashing myID in init");
        }
        PREDECESSORID = MYID;
        SUCCESSORID = MYID;
        try{
            LEADERID = genHash(Integer.parseInt(LEADERPORT)/2+"");
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG,"NoSuchAlgorithmException when INITING leader");
        }

        Log.e(TAG,"Initing IDs: SUCCESSOR:"+SUCCESSORID+" SUCCESSOR PORT:"+SUCCESSORPORT+" MYID:"
                +MYID+" MyPort:"+MYPORT+" PREDECESSOR:"+PREDECESSORID+" PREDECESSORPort:"
                +PREDECESSORPORT+" LEADER:"+LEADERID+" LeaderPort:"+LEADERPORT);

    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

//        protected void onProgressUpdate(String... strings) {
//            return;
//        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            while (TRUE) {
                try {
                    Socket cliSock = serverSocket.accept();
                    String str;

                    BufferedReader br = new BufferedReader(new InputStreamReader(cliSock.getInputStream()));
                    do {
                        str = br.readLine();
                    } while (str == null);

                    String ret = "";
                    int msgType = Integer.parseInt(str.split(DELIM)[0]);

                    if(msgType == JOINREQ){
                        Log.e(TAG,"Got Join Req: "+str);
                        ret = addNode(str);
                        Log.e(TAG,"Included new node into NW. Sending neighbours of new node. N: "+ret);
//                        printNodeStatus();
                    }else if (msgType == UPDATENEIGHBOURS){
                        Log.e(TAG,"Got update req "+str);
                        ret = updateNeighbours(str);
                        Log.e(TAG,"Updated Neighbour info");
                        printNeighbours();
                    }else if (msgType == INSERTMSG){
                        Log.e(TAG,"Got forwarded MSG for insertion["+str+"]");
                        ret = insertOrForward(str);
                    }else if (msgType == DELETEMSG) {
                        Log.e(TAG,"Got forwarded MSG for deletion["+str+"]");
                        ret = deleteOrForward(str);
                    }else if (msgType ==QUERYMSG){
                        Log.e(TAG,"Got forwarded MSQ for query["+str+"]");
                        ret = queryOrForward(str);

                    }

                    PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
                    ackSender.println(ret);

//                    publishProgress(str);
                    br.close();
                    cliSock.close();
                } catch (IOException e) {
                    Log.e(TAG,"Error when reading and publishingProgress");
                    e.printStackTrace();
                }


            }


            return null;
        }

        private String queryOrForward(String str) {
            String[] msgParams = str.split(DELIM);
            String key = msgParams[2];
            String cursorStr = "";
            Cursor tempCursor;
            if(key.equals("*")){
                tempCursor = query(mUri,null,"@",null,null);
                if(!msgParams[4].equals(SUCCESSORPORT)) {
                    msgParams[1] = SUCCESSORPORT;
                    String msg = msgParams[0] + DELIM + msgParams[1] + DELIM + msgParams[2] + DELIM + msgParams[3] + DELIM + msgParams[4];
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                    while(QUERYRETURNEDFLAG==false);
                    final Cursor mergedCursor = new MergeCursor(new Cursor[] {
                            tempCursor,
                            QUERYGLOBALOUTPUT
                    });
                    QUERYRETURNEDFLAG = false;
                    QUERYGLOBALOUTPUT = null;
                    Log.e(TAG,"Apendded local and retrieved cursors from other AVDs:["+mergedCursor.toString()+"]");
                    tempCursor = mergedCursor;

                }else {
                    Log.e(TAG,"Got select * Query from "+msgParams[4]+" Collected results from everywhere. Now in final port:"+MYPORT);
                }
            }else{
                tempCursor = query(mUri,null,key,null,null);

            }
            cursorStr = convertCursorToString(tempCursor);
            return ACKSTR+DELIM+cursorStr;
        }

        private String deleteOrForward(String str) {
            String[] msgParams = str.split(DELIM);
            String key = msgParams[2];
            if(key.equals("*")){
                delete(mUri,"@",null);
                if(!msgParams[4].equals(SUCCESSORPORT)) {
                    msgParams[1] = SUCCESSORPORT;
                    String msg = msgParams[0] + DELIM + msgParams[1] + DELIM + msgParams[2] + DELIM + msgParams[3] + DELIM + msgParams[4];
                    Log.e(TAG,"Got del * Query from "+msgParams[4]+" Deleted locally, forwarding to next");
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
                    while(DELRETURNEDFLAG == false);
                    DELRETURNEDFLAG = false;
                }else {
                    Log.e(TAG,"Got del * Query from "+msgParams[4]+" Deleted everywhere. Now in final port:"+MYPORT);
                }
            }else{
                delete(mUri,key,null);
            }
            return ACKSTR+"";
        }

        private String insertOrForward(String str) {
            String key = str.split(DELIM)[2];
            String val = str.split(DELIM)[3];
//            if (shouldIforward(key)){
//                return ACKSTR+"";
//            }else {
            ContentValues mContentValues = new ContentValues();

            mContentValues.put(COLUMN_KEY, key);
            mContentValues.put(COLUMN_VAL, val);
            try {
                Log.e(TAG,"Got key:"+key+" from:"+str.split(DELIM)[4]+" will try to insert locally");
                insert(mUri, mContentValues);
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return ACKSTR + "";
            }
            return ACKSTR + "";
//            }
        }

        private String addNode(String str) {
            String ret = UPDATENEIGHBOURS + DELIM;
            String neighbID = str.split(DELIM)[1];
            String neighbPort = str.split(DELIM)[2];

            String newNofClient = updateNodeTables(neighbID,neighbPort);
            ret += newNofClient;

            return ret;
        }

        private String updateNodeTables(String cliID, String cliPort) {
            Log.e(TAG,"getting hashed index of port: "+cliPort);

            int nodeIndex = nodePortIndexMap.get(cliPort);
            String succID ="";
            String succPort ="";
            String predID ="";
            String predPort ="";
            if (FIRSTNODE) {
                //first insertion -- cli is succ and pred
                nodeTable[0].successorId = cliID;
                nodeTable[0].successorPort = cliPort;
                nodeTable[0].predecessorId = cliID;
                nodeTable[0].predecessorPort = cliPort;
                succID = MYID;
                succPort = MYPORT;
                predID = succID;
                predPort = succPort;
                if (succID.compareTo(cliID) > 0) {
                    HEADID = cliID;
                    HEADPORT = cliPort;
                } else {
                    TAILID = cliID;
                    TAILPORT = cliPort;
                }
                Log.e(TAG,"Inserting into NW -- second NODE in NW: H:"+HEADPORT+" T:"+TAILPORT+" S:"+succPort+" P:"+predPort);
                FIRSTNODE = false;

            } else {
                if (cliID.compareTo(HEADID) < 0) {
                    //this will be first node
                    //update node_table
                    Log.e(TAG,"getting hashed index of port: "+HEADPORT + " and :"+TAILPORT);

                    int succInd = nodePortIndexMap.get(HEADPORT);
                    int predInd = nodePortIndexMap.get(TAILPORT);
                    nodeTable[succInd].predecessorId = cliID;
                    nodeTable[succInd].predecessorPort = cliPort;
                    nodeTable[predInd].successorId = cliID;
                    nodeTable[predInd].successorPort = cliPort;
                    //node_table updated
                    succID = HEADID;
                    succPort = HEADPORT;
                    predID = TAILID;
                    predPort = TAILPORT;

                    HEADID = cliID;
                    HEADPORT = cliPort;
                    Log.e(TAG,"Inserting into NW -- as the head : H:"+HEADPORT+" T:"+TAILPORT+" S:"+succPort+" P:"+predPort);

                } else if (cliID.compareTo(TAILID) > 0) {
                    //this will be last node
                    //update node_table
                    Log.e(TAG,"getting hashed index of port: "+HEADPORT + " and :"+TAILPORT);
                    int succInd = nodePortIndexMap.get(HEADPORT);
                    int predInd = nodePortIndexMap.get(TAILPORT);
                    nodeTable[succInd].predecessorId = cliID;
                    nodeTable[succInd].predecessorPort = cliPort;
                    nodeTable[predInd].successorId = cliID;
                    nodeTable[predInd].successorPort = cliPort;
                    //node_table updated
                    succID = HEADID;
                    succPort = HEADPORT;
                    predID = TAILID;
                    predPort = TAILPORT;

                    TAILID = cliID;
                    TAILPORT = cliPort;
                    Log.e(TAG,"Inserting into NW -- as the tail : H:"+HEADPORT+" T:"+TAILPORT+" S:"+succPort+" P:"+predPort);


                } else {
                    //somewhere in the middle
                    String tempPtr = HEADPORT;
                    while (tempPtr != TAILPORT) {
                        Log.e(TAG,"getting hashed index of port: "+tempPtr);
                        int iPtr = nodePortIndexMap.get(tempPtr);
                        if (nodeTable[iPtr].successorId.compareTo(cliID) > 0) {

                            succID = nodeTable[iPtr].successorId;
                            succPort = nodeTable[iPtr].successorPort;
                            predID = nodeTable[iPtr].portID;
                            predPort = nodeTable[iPtr].port;
                            Log.e(TAG,"Inserting into NW -- in the middle : H:"+HEADPORT+" T:"+TAILPORT+" S:"+succPort+" P:"+predPort);
                            //update node_table
                            Log.e(TAG,"getting hashed index of port: "+succPort + " and :"+predPort);

                            int succInd = nodePortIndexMap.get(succPort);
                            int predInd = nodePortIndexMap.get(predPort);
                            nodeTable[succInd].predecessorId = cliID;
                            nodeTable[succInd].predecessorPort = cliPort;
                            nodeTable[predInd].successorId = cliID;
                            nodeTable[predInd].successorPort = cliPort;
                            //node_table updated
                            break;
                        } else {
                            tempPtr = nodeTable[iPtr].successorPort;
                        }
                    }

                }
            }


            nodeTable[nodeIndex].successorId = succID;
            nodeTable[nodeIndex].successorPort = succPort;
            nodeTable[nodeIndex].predecessorId = predID;
            nodeTable[nodeIndex].predecessorPort = predPort;
            nodeTable[nodeIndex].portID = cliID;
            nodeTable[nodeIndex].port = cliPort;
            nodeTable[nodeIndex].active = true;

            Log.e(TAG,"Added new node:["+cliID+","+cliPort+"]. Its neighbours are: S["+succID+","+succPort+"] P["+predID+","+predPort+"]");
            return succID+DELIM+succPort+DELIM+predID+DELIM+predPort;
        }

        private String updateNeighbours(String str) {
            String neighbourID = str.split(DELIM)[2];
            String neighbourPort = str.split(DELIM)[3];
            int changeSucc = Integer.parseInt(str.split(DELIM)[4]);
            if(SUCCESSORID.equals(LEADERID)&&PREDECESSORID.equals(LEADERID)&&MYID.equals(LEADERID)){
                //leader's initial state

                Log.e(TAG,"Updating Leader's Succ and Pred as:"+neighbourPort);
                SUCCESSORID = neighbourID;
                SUCCESSORPORT = neighbourPort;
                PREDECESSORID = neighbourID;
                PREDECESSORPORT = neighbourPort;
            }else {
                if (changeSucc == 1) {
                    Log.e(TAG,"Updating Succ as:"+neighbourPort);

                    SUCCESSORID = neighbourID;
                    SUCCESSORPORT = neighbourPort;
                } else {
                    Log.e(TAG,"Updating Pred as:"+neighbourPort);

                    PREDECESSORID = neighbourID;
                    PREDECESSORPORT = neighbourPort;
                }
            }
            return (ACKSTR+"");
        }


    }

    private String convertCursorToString(Cursor tempCursor) {
        String ret="";
        tempCursor.moveToFirst();
        while(tempCursor.isAfterLast()== false){
            ret = ret+tempCursor.getString(0)+CURSORDELIM+tempCursor.getString(1)+CURSORDELIM;
            tempCursor.moveToNext();
        }
        return ret;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            msgToSend = msgToSend.replace("\n", "");
            String msgStrings[] = msgToSend.split(DELIM);
            int msgType = Integer.parseInt(msgStrings[0]);
            String ret;

            if ( msgType == JOINREQ){
                String neighbours = sendJoinReq();
                if(neighbours!= null){
                    String n[] = neighbours.split(DELIM);
                    SUCCESSORID = n[1];
                    SUCCESSORPORT = n[2];
                    PREDECESSORID = n[3];
                    PREDECESSORPORT = n[4];
                }
                UPDATEFLAG = true;

            }else if(msgType == UPDATENEIGHBOURS || msgType ==INSERTMSG || msgType == DELETEMSG ){
                ret = sendMsgToANode(msgToSend);
                if(msgType == DELETEMSG){
                    DELRETURNEDFLAG = true;
                }
            }else if(msgType == QUERYMSG) {
                ret = sendMsgToANode(msgToSend);
                String queryResStr[] = ret.split(DELIM);
                if (queryResStr.length == 2) {
                    Cursor tempCursor = convertStringToCursor(queryResStr[1]);
                    QUERYGLOBALOUTPUT = tempCursor;
                }
                QUERYRETURNEDFLAG = true;

            }

            return null;
        }

        private String sendMsgToANode(String msg) {
            try {
                String destPort = msg.split(DELIM)[1];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(destPort));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                out.println(msg);
                Log.e(TAG,"Updating Neighbour:"+destPort+" With:["+msg+"]");

                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String reply;
//                long t1 = System.currentTimeMillis();
                do {
                    reply = br.readLine();
//                    long t2 = System.currentTimeMillis();
//                    if (t2-t1>1500){
//                        return null;
//                    }
                }while (reply==null);
                socket.close();
                return reply;
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }

        private String sendJoinReq() {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(LEADERPORT));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                String msg = JOINREQ+DELIM+MYID+DELIM+MYPORT;
                out.println(msg);

                Log.e(TAG,"Sent Join Req:["+msg+"] to "+LEADERPORT);
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String reply;
                long t1 = System.currentTimeMillis();
                do {
                    reply = br.readLine();
                    long t2 = System.currentTimeMillis();
                    if (t2-t1>500){
                        return null;
                    }
                }while (reply==null);
                int ackMsgType = Integer.parseInt(reply.split(DELIM)[0]);
                if(ackMsgType == UPDATENEIGHBOURS){
                    Log.e(TAG,"Got Resp:["+reply+"]");
                    return reply;
                }
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }


    }

    private Cursor convertStringToCursor(String queryResStr) {
        MatrixCursor tempMC = new MatrixCursor(new String[] {COLUMN_KEY,COLUMN_VAL});
        String cursorElem[] = queryResStr.split(CURSORDELIM);
        for (int i = 0; i<cursorElem.length;i+=2 ){
            String array[] = new String[2];
            array[0] = cursorElem[i];
            array[1] = cursorElem[i+1];
            tempMC.addRow(array);

        }
        return (Cursor)tempMC;
    }

}
