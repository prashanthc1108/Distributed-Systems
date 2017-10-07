package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.PriorityBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static edu.buffalo.cse.cse486586.simpledynamo.MySQLiteOpenHelper.COLUMN_KEY;
import static edu.buffalo.cse.cse486586.simpledynamo.MySQLiteOpenHelper.COLUMN_OWNER;
import static edu.buffalo.cse.cse486586.simpledynamo.MySQLiteOpenHelper.COLUMN_VAL;
import static edu.buffalo.cse.cse486586.simpledynamo.MySQLiteOpenHelper.COLUMN_VERSION;
import static edu.buffalo.cse.cse486586.simpledynamo.MySQLiteOpenHelper.TABLE_NAME;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class SimpleDynamoProvider extends ContentProvider {

	private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider");


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	MySQLiteOpenHelper mySQLAccess = null;
	static final int NODECOUNT = 5;
	static boolean RECOVERED = false;
	static boolean SUCCESSORCOPIED = false;
	static boolean PREDECESSORCOPIED = false;

	private final Object InsertLock = new Object();

	//	public static final String REMOTE_PORTS[] = {"11108","11112","11116","11120","11124"};
	public static final String REMOTE_PORTS[] = {"11124", "11112", "11108", "11116", "11120"};
	public static final String REMOTE_IDS[] = new String[NODECOUNT];
	public static final String otherNodes[] = new String[NODECOUNT-1];
	public static final String otherIDs[] = new String[NODECOUNT-1];
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String DELIM = " ";
	static String SUCCESSOR1PORT = "";
	static String SUCCESSOR1ID = "";
	static String PREDECESSOR1PORT = "";
	static String PREDECESSOR1ID = "";
	static String SUCCESSOR2PORT = "";
	static String SUCCESSOR2ID = "";
	static String PREDECESSOR2PORT = "";
	static String PREDECESSOR2ID = "";
	static String MYID = "";
	static String MYPORT = "";

	public static HashMap<String, Integer> nodePortIndexMap = new HashMap<String, Integer>();

	//MSG TYPES:
	public static int ACKSTR = 0;
	public static int INSERTMSG = 1;
	public static int QUERYMSG = 2;
	public static int DELETEMSG = 3;
	public static int QRESPMSG = 4;
	public static int UPDATEREQ = 5;
	public static int UPDATERESP = 6;
	public static int UQUERYMSG = 7;


	//Query lock flags
//	public static boolean QUERYFLAG[] = {FALSE,FALSE,FALSE,FALSE};
//	public static Cursor QUERYGLOBALOUTPUT[] = new Cursor[NODECOUNT-1];
//	public static boolean QUERYFLAGIND = FALSE;
//	public static Cursor QUERYGLOBALIND;
	static final String CURSORDELIM = ",";
	public static HashMap<String, Cursor> remoteCursor = new HashMap<String, Cursor>();
	public static HashMap<String, Boolean> remoteCursorFlag = new HashMap<String, Boolean>();
	public static HashMap<String, Boolean> queryReqTimeout = new HashMap<String, Boolean>();




	private void setupTopology() {
		for (int i = 0; i < 5; i++) {
			nodePortIndexMap.put(REMOTE_PORTS[i], i);
			try {
				REMOTE_IDS[i] = genHash(Integer.parseInt(REMOTE_PORTS[i])/2+"");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		getMyPort();
		int myInd, s1Ind, s2Ind, p1Ind, p2Ind;
		myInd = nodePortIndexMap.get(MYPORT);
		s1Ind = (myInd + NODECOUNT + 1) % NODECOUNT;
		s2Ind = (myInd + NODECOUNT + 2) % NODECOUNT;
		p1Ind = (myInd + NODECOUNT - 1) % NODECOUNT;
		p2Ind = (myInd + NODECOUNT - 2) % NODECOUNT;
//		Log.e(TAG,"Index.. P2:"+p2Ind+" P1:"+p1Ind+" Me:"+myInd+" S1:"+s1Ind+" S2:"+s2Ind);

		MYID = REMOTE_IDS[myInd];
		SUCCESSOR1PORT = REMOTE_PORTS[s1Ind];
		SUCCESSOR1ID = REMOTE_IDS[s1Ind];
		SUCCESSOR2PORT = REMOTE_PORTS[s2Ind];
		SUCCESSOR2ID = REMOTE_IDS[s2Ind];
		PREDECESSOR1ID = REMOTE_IDS[p1Ind];
		PREDECESSOR1PORT = REMOTE_PORTS[p1Ind];
		PREDECESSOR2ID = REMOTE_IDS[p2Ind];
		PREDECESSOR2PORT = REMOTE_PORTS[p2Ind];
		otherNodes[0] = PREDECESSOR2PORT;
		otherNodes[1] = PREDECESSOR1PORT;
		otherNodes[2] = SUCCESSOR1PORT;
		otherNodes[3] = SUCCESSOR2PORT;
		try {
			otherIDs[0] = genHash(otherNodes[0]);
			otherIDs[1] = genHash(otherNodes[1]);
			otherIDs[2] = genHash(otherNodes[2]);
			otherIDs[3] = genHash(otherNodes[3]);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

//		Log.e(TAG,"Init done.. P2:"+PREDECESSOR2PORT+" P1:"+PREDECESSOR1PORT+" Me:"+MYPORT+" S1:"+SUCCESSOR1PORT+" S2:"+SUCCESSOR2PORT);
	}

	private void getMyPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		MYPORT = (Integer.parseInt(portStr) * 2) + "";
//		Log.e(TAG,"Myport:"+MYPORT);
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

	@Override
	public boolean onCreate() {

		setupTopology();
//		assertIDs();
		mySQLAccess = new MySQLiteOpenHelper(getContext());

		createServerTask();
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		RECOVERED = false;

		recoverDB();

		//		updateMyDB();

//		SQLiteDatabase sqlDB = mySQLAccess.getWritableDatabase();

		Log.e(TAG, "SERVER UP!");

		return false;
	}

	private void recoverDB() {
		SQLiteDatabase sqlDB;// = mySQLAccess.getWritableDatabase();
		synchronized (InsertLock) {
			sqlDB = mySQLAccess.getWritableDatabase();
			sqlDB.rawQuery("delete from " + TABLE_NAME, null);
		}
		SUCCESSORCOPIED = false;
		PREDECESSORCOPIED = false;
//		copyFromNode(SUCCESSOR1PORT);
//		copyFromNode(PREDECESSOR1PORT);
		Thread replicateSuccessor = new Thread(new ReplicateNode(SUCCESSOR1PORT));
		replicateSuccessor.start();
		Thread replicatePredecessor = new Thread(new ReplicateNode(PREDECESSOR1PORT));
		replicatePredecessor.start();
		while(!SUCCESSORCOPIED){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};
		while (!PREDECESSORCOPIED){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};
		RECOVERED = true;
	}

	public class ReplicateNode implements Runnable{

		String destPort;
		public ReplicateNode(String str){
			this.destPort = str;
		}
		public void run(){
			String selection = "*";
			String cursorHashKey = selection+"_"+destPort;
			remoteCursorFlag.put(cursorHashKey,false);
			queryReqTimeout.put(cursorHashKey, false);
			forwardUpdateReqToNode(selection,destPort);
			Cursor insertCursor = null;
//		while (true) {
			Log.e(TAG,"Will copy from "+destPort+" waiting for response");

			while (remoteCursorFlag.get(cursorHashKey)==false && queryReqTimeout.get(cursorHashKey)==false){
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			};
//			if (queryReqTimeout.get(cursorHashKey)) {
//				queryReqTimeout.remove(cursorHashKey);
//				remoteCursorFlag.remove(cursorHashKey);
//				if(destPort.equals(SUCCESSOR1PORT)){
//					destPort = SUCCESSOR2PORT;
//				}else {
//					destPort = PREDECESSOR2PORT;
//				}
//				cursorHashKey = selection + "_" + destPort;
//				remoteCursorFlag.put(cursorHashKey, false);
//				queryReqTimeout.put(cursorHashKey, false);
//				forwardUpdateReqToNode(selection, destPort);
//				continue;
//			}
			if (remoteCursor.containsKey(cursorHashKey)) {
				Log.e(TAG,"Got response");
				insertCursor = remoteCursor.get(cursorHashKey);
				synchronized (InsertLock) {
					insertCursorToDB(insertCursor);
				}

				remoteCursor.remove(cursorHashKey);

			}else{
				Log.e(TAG,"Timed out");

			}
			remoteCursorFlag.remove(cursorHashKey);
			queryReqTimeout.remove(cursorHashKey);
			if(destPort.equals(SUCCESSOR1PORT)||destPort.equals(SUCCESSOR2PORT)){
				SUCCESSORCOPIED = true;
			}else{
				PREDECESSORCOPIED = true;
			}
//			break;
//		}

		}

	}

	private void copyFromNode(String destPort) {
		String selection = "*";
		String cursorHashKey = selection+"_"+destPort;
		remoteCursorFlag.put(cursorHashKey,false);
		queryReqTimeout.put(cursorHashKey, false);
		forwardUpdateReqToNode(selection,destPort);
		Cursor insertCursor = null;
//		while (true) {
			Log.e(TAG,"Will copy from "+destPort+" waiting for response");

			while (!remoteCursorFlag.get(cursorHashKey) && !queryReqTimeout.get(cursorHashKey)) {
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			};
//			if (queryReqTimeout.get(cursorHashKey)) {
//				queryReqTimeout.remove(cursorHashKey);
//				remoteCursorFlag.remove(cursorHashKey);
//				if(destPort.equals(SUCCESSOR1PORT)){
//					destPort = SUCCESSOR2PORT;
//				}else {
//					destPort = PREDECESSOR2PORT;
//				}
//				cursorHashKey = selection + "_" + destPort;
//				remoteCursorFlag.put(cursorHashKey, false);
//				queryReqTimeout.put(cursorHashKey, false);
//				forwardUpdateReqToNode(selection, destPort);
//				continue;
//			}
			if (remoteCursor.containsKey(cursorHashKey)) {
				Log.e(TAG,"Got response");
				insertCursor = remoteCursor.get(cursorHashKey);
				insertCursorToDB(insertCursor);

				remoteCursor.remove(cursorHashKey);

			}else{
				Log.e(TAG,"Timed out");

			}
			remoteCursorFlag.remove(cursorHashKey);
			queryReqTimeout.remove(cursorHashKey);
		if(destPort.equals(SUCCESSOR1PORT)||destPort.equals(SUCCESSOR2PORT)){
			SUCCESSORCOPIED = true;
		}else{
			PREDECESSORCOPIED = true;
		}
//			break;
//		}
	}

	private void insertCursorToDB(Cursor tempCursor) {
		String ret="";
		tempCursor.moveToFirst();
		SQLiteDatabase sqlDB = mySQLAccess.getWritableDatabase();
		while(tempCursor.isAfterLast()== false){
			ContentValues values = new ContentValues();
			values.put(COLUMN_KEY, tempCursor.getString(0));
			values.put(COLUMN_VAL, tempCursor.getString(1));
			synchronized (InsertLock) {
				sqlDB.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
			}
			tempCursor.moveToNext();
		}


	}

	private void forwardUpdateReqToNode(String selection, String destNode) {
		String msg = UPDATEREQ+ DELIM+ destNode+DELIM+MYPORT+DELIM+selection+DELIM+selection+"_"+destNode;
		Log.e(TAG,"Forwarding Query for key["+selection+"] to "+destNode+"as Msg:["+msg+"]");
		Thread fwdUpdateThread = new Thread(new ClientTask(msg));
		fwdUpdateThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

	}

	private void assertIDs() {
		boolean state = false;
		for (int i = 0; i<4; i++){
			if(REMOTE_IDS[i].compareTo(REMOTE_IDS[i+1])>0){
				state = true;
				Log.e(TAG, "Improper set up: Node["+i+"]("+REMOTE_IDS[i]+"("+REMOTE_PORTS[i]+"))>Node["+(i+1)+"]("+REMOTE_IDS[i+1]+"("+REMOTE_PORTS[i+1]+"))");
			}
		}
		if(REMOTE_IDS[4].compareTo(REMOTE_IDS[0])<0){
			state = true;
			Log.e(TAG, "Improper set up: Node[0]("+REMOTE_IDS[0]+"("+REMOTE_PORTS[0]+"))>Node[4]("+REMOTE_IDS[4]+"("+REMOTE_PORTS[4]+"))");
		}
		if(state){
			Log.e(TAG, "naat ferfect");
		}
		else{
			Log.e(TAG, "ferfect");
		}
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		while (!RECOVERED){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		String[] delKeys = {selection};
		SQLiteDatabase sqlDB;
		if(selection.equals("@")){
			synchronized (InsertLock) {
				Log.e(TAG, "Delete all local messages");
				sqlDB = mySQLAccess.getWritableDatabase();
				sqlDB.rawQuery("delete from " + TABLE_NAME, null);
			}
		}else if(selection.equals("*")) {
			synchronized (InsertLock) {
				Log.e(TAG, "Delete * :all local messages and also delete from others");
				sqlDB = mySQLAccess.getWritableDatabase();
				sqlDB.rawQuery("delete from " + TABLE_NAME, null);
			}
			deleteStar();
		}else{
			String destPort = getCoordinator(selection);
			if(destPort.equals(MYPORT)){
				synchronized (InsertLock) {
					sqlDB = mySQLAccess.getWritableDatabase();
					sqlDB.delete(TABLE_NAME, "key = ?", delKeys);
				}
				String msg = DELETEMSG+DELIM+SUCCESSOR1PORT+DELIM+selection;
				Thread delete1Thread = new Thread(new ClientTask(msg));
				delete1Thread.start();
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
				msg = DELETEMSG+DELIM+SUCCESSOR2PORT+DELIM+selection;
				Thread delete2Thread = new Thread(new ClientTask(msg));
				delete2Thread.start();
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
			}else{
				forwardKeyToDelete(selection,destPort);
			}

		}
		return 0;

	}

	private void deleteStar() {
		String msg = DELETEMSG+DELIM+SUCCESSOR1PORT+DELIM+"*";
		Thread delete1starThread = new Thread(new ClientTask(msg));
		delete1starThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		msg = DELETEMSG+DELIM+SUCCESSOR2PORT+DELIM+"*";
		Thread delete2starThread = new Thread(new ClientTask(msg));
		delete2starThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		msg = DELETEMSG+DELIM+PREDECESSOR1PORT+DELIM+"*";
		Thread delete3starThread = new Thread(new ClientTask(msg));
		delete3starThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		msg = DELETEMSG+DELIM+PREDECESSOR2PORT+DELIM+"*";
		Thread delete4starThread = new Thread(new ClientTask(msg));
		delete4starThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
	}

	private void forwardKeyToDelete(String delKeys, String destPort) {
		String msg = DELETEMSG + DELIM + destPort + DELIM + delKeys;
		Thread fwdToDelThread = new Thread(new ClientTask(msg));
		fwdToDelThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		destPort = getSuccessor(destPort, 1);
		if (destPort.equals(MYPORT)) {
			SQLiteDatabase sqlDB;// = mySQLAccess.getWritableDatabase();
			String[] keys = {delKeys};
			synchronized (InsertLock) {
				sqlDB = mySQLAccess.getWritableDatabase();
				sqlDB.delete(TABLE_NAME, "key = ?", keys);
			}
		} else {
			msg = DELETEMSG + DELIM + destPort + DELIM + delKeys;
			Thread fwdToDel1Thread = new Thread(new ClientTask(msg));
			fwdToDel1Thread.start();
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		}
		destPort = getSuccessor(destPort, 1);
		if (destPort.equals(MYPORT)) {
			SQLiteDatabase sqlDB;// = mySQLAccess.getWritableDatabase();
			String[] keys = {delKeys};
			synchronized (InsertLock) {
				sqlDB = mySQLAccess.getWritableDatabase();
				sqlDB.delete(TABLE_NAME, "key = ?", keys);
			}
		} else {
			msg = DELETEMSG + DELIM + destPort + DELIM + delKeys;
			Thread fwdToDel2Thread = new Thread(new ClientTask(msg));
			fwdToDel2Thread.start();
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
		}
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		while (!RECOVERED){
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


		String key = (String) values.get(COLUMN_KEY);
		String val = (String) values.get(COLUMN_VAL);
//		String owner = null;
//		String verion = null;
//		if(values.containsKey(COLUMN_OWNER)) {
//			owner = (String) values.get(COLUMN_OWNER);
//			verion = (String) values.get(COLUMN_VERSION);
//		}
		String destPort = getCoordinator(key);
//		Log.e(TAG, "Key ["+key+"] arrived for insert, should be inserted in:"+destPort);

//		if (!(destPort.equals(MYPORT))) {
			Log.e(TAG, "Key[" + key + "] should be inserted into port:" + destPort);
//			key = key + DELIM + val + DELIM + destPort;
			forwardKeyToInsert(key,val,destPort,true);
//		} else {
//			//TODO use lock
//			Log.e(TAG, "Key[" + key + "] should be inserted into Self Port:" + destPort);
//			forceInsert(values,destPort);
//			forwardKeyToInsert(key,val,destPort,false);
////			key = key + DELIM + val + DELIM + destPort;
////			forwardKeyToInsert(key,val,destPort,true);
//		}

		return uri;
	}

	private void forwardKeyToInsert(String key, String val, String destPort, boolean sendToCoordinator) {
		String msg;
//		if(sendToCoordinator) {
			msg = INSERTMSG+DELIM+destPort+DELIM+key+DELIM+val+DELIM+destPort;
			Log.e(TAG, " Forwarding Key[" + key + "] to :" + destPort+"as Msg:["+msg+"]");
			Thread fwdToInsThread = new Thread(new ClientTask(msg));
			fwdToInsThread.start();
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
//		}
		String nextPort = getSuccessor(destPort,1);
//		if(!(nextPort.equals(MYPORT))) {
			msg = INSERTMSG + DELIM + nextPort + DELIM + key+DELIM+val+DELIM+destPort;
			Log.e(TAG, " Forwarding Key[" + key + "] for replication 1 to :" + nextPort+"as Msg:["+msg+"]");
			Thread fwdToIns1Thread = new Thread(new ClientTask(msg));
			fwdToIns1Thread.start();
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
//		}else{
//			ContentValues values = new ContentValues();
//			values.put(COLUMN_KEY,key);
//			values.put(COLUMN_VAL,val);
//			Log.e(TAG,"I am("+MYPORT+") replication node 1 for key:"+ key);
//			forceInsert(values,destPort);
//		}
		nextPort = getSuccessor(destPort,2);
//		if(!(nextPort.equals(MYPORT))) {
			msg = INSERTMSG + DELIM + nextPort + DELIM + key+DELIM+val+DELIM+destPort;
			Log.e(TAG, " Forwarding Key[" + key + "] for replication 2 to :" + nextPort+"as Msg:["+msg+"]");
			Thread fwdToIns2Thread = new Thread(new ClientTask(msg));
			fwdToIns2Thread.start();
//			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
//		}else{
//			ContentValues values = new ContentValues();
//			values.put(COLUMN_KEY,key);
//			values.put(COLUMN_VAL,val);
//			Log.e(TAG,"I am("+MYPORT+") replication node 2 for key:"+ key);
//			forceInsert(values,destPort);
//		}
	}

	private String getSuccessor(String destPort, int i) {
		return REMOTE_PORTS[(nodePortIndexMap.get(destPort)+i)%NODECOUNT];
	}

	private void forceInsert(ContentValues values, String destPort) {
		synchronized (InsertLock) {
			String key = (String) values.get(COLUMN_KEY);
			String val = (String) values.get(COLUMN_VAL);
			SQLiteDatabase sqlDB = mySQLAccess.getWritableDatabase();
			int version = 0;

//		if (values.containsKey(COLUMN_OWNER)) {
//			int queryVersion = Integer.parseInt((String) values.get(COLUMN_VERSION));
//			version = getVersionifKeyExists(key);
//			if (queryVersion > version) {
//				sqlDB.insert(TABLE_NAME, null, values);
//			}
//		} else {
//			version = 0;
//			version = getVersionifKeyExists(key);
//			if (version > 0) {
//				Log.e(TAG, "Updating key[" + key + "]");
//				String[] delKeys = {key};
//				sqlDB.delete(TABLE_NAME, "key = ?", delKeys);
//			}
//			version++;
//			values.put(COLUMN_VERSION, version);
//			values.put(COLUMN_OWNER, destPort);
//			Log.e(TAG, "Inserting into DB -- Key:" + key + " Val:" + val + " Owner:" + destPort + " version:" + version);
//			sqlDB.insert(TABLE_NAME, null, values);
//		}
			Log.e(TAG, "Inserting into DB -- Key:" + key + " Val:" + val + " Owner:" + destPort + " version:" + version);
//		String[] delKeys = {k`ey};sqlDB.delete(TABLE_NAME, "key = ?", delKeys);
//			synchronized (InsertLock) {
				sqlDB.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
//			}
		}
	}



	private int getVersionifKeyExists(String key) {
		String[] projection = {COLUMN_VERSION};
		Cursor tempCursor = query(mUri, projection, key, null, null);
		if (tempCursor.getCount() == 0) {
			return 0;
		} else {
			tempCursor.moveToFirst();
			return Integer.parseInt(tempCursor.getString(tempCursor.getColumnIndex(COLUMN_VERSION)));
		}
	}

	private String getCoordinator(String key) {
		String hashedKey = null;
		String retPort = MYPORT;
		try {
			hashedKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if (hashedKey.compareTo(REMOTE_IDS[0]) <= 0) {
			retPort = REMOTE_PORTS[0];
		} else if (hashedKey.compareTo(REMOTE_IDS[4]) > 0) {
			retPort = REMOTE_PORTS[0];
		}

		for (int i = 0; i < 4; i++) {
			if ((hashedKey.compareTo(REMOTE_IDS[i]) > 0) && (hashedKey.compareTo(REMOTE_IDS[i + 1]) <= 0)) {
				retPort = REMOTE_PORTS[i + 1];
			}
		}
//		Log.e(TAG, "Coordinator of key["+key+"] is:"+retPort);
		return retPort;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		//TODO protect query?
//		while(!RECOVERED);
		Cursor queryCursor = null;

		if(!RECOVERED){
			String coord = getCoordinator(selection);
			if(coord.equals(MYPORT)) {
				coord = SUCCESSOR1PORT;
			}
//			while (true) {
				String cursorHashKey = selection + "_" + coord;
				remoteCursorFlag.put(cursorHashKey, false);
				queryReqTimeout.put(cursorHashKey, false);
				forwardUQueryToNode(selection, coord);

				while (!remoteCursorFlag.get(cursorHashKey) && !queryReqTimeout.get(cursorHashKey)) {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				;
				if (queryReqTimeout.get(cursorHashKey)) {
					queryReqTimeout.remove(cursorHashKey);
					remoteCursorFlag.remove(cursorHashKey);
					coord = getSuccessor(coord,1);
					cursorHashKey = selection + "_" + coord;
					remoteCursorFlag.put(cursorHashKey, false);
					queryReqTimeout.put(cursorHashKey, false);
					forwardUQueryToNode(selection, coord);
					while (!remoteCursorFlag.get(cursorHashKey) && !queryReqTimeout.get(cursorHashKey)) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				if (remoteCursor.containsKey(cursorHashKey)) {

					queryCursor = remoteCursor.get(cursorHashKey);

					remoteCursor.remove(cursorHashKey);
				}
					remoteCursorFlag.remove(cursorHashKey);
					queryReqTimeout.remove(cursorHashKey);

//			}
		}else {

			String[] queryColumnsVals = {selection};
			SQLiteDatabase sqlDB;// = mySQLAccess.getReadableDatabase();
			String[] queryProjection = {COLUMN_KEY, COLUMN_VAL};
			if (projection != null) {
				queryProjection = projection;
			}

			if (selection.equals("@")) {
				synchronized (InsertLock) {
					Log.e(TAG, "Query all local messages");
					//TODO * or key,val?
					sqlDB = mySQLAccess.getReadableDatabase();
					queryCursor = sqlDB.rawQuery("select " + COLUMN_KEY + "," + COLUMN_VAL + " from " + TABLE_NAME, null);
				}
//			Log.e(TAG, "Query all local messages, result:["+convertCursorToString(queryCursor)+"]");
			} else if (selection.equals("*")) {
				Cursor tempCursor = null;
				synchronized (InsertLock) {
					Log.e(TAG, "Query all messages from all nodes");
					//TODO * or key,val?
					sqlDB = mySQLAccess.getReadableDatabase();
					tempCursor = sqlDB.rawQuery("select " + COLUMN_KEY + "," + COLUMN_VAL + " from " + TABLE_NAME, null);
					Log.e(TAG, "Got all local entries from DB:[" + tempCursor.toString() + "], now forward to others");
				}
				for (int i = 0; i < NODECOUNT - 1; i++) {
					String cursorHashKey = selection + "_" + otherNodes[i];
					remoteCursorFlag.put(cursorHashKey, false);
					queryReqTimeout.put(cursorHashKey, false);
					forwardQueryToNode(selection, otherNodes[i]);
				}
				for (int i = 0; i < NODECOUNT - 1; i++) {
					String cursorHashKey = selection + "_" + otherNodes[i];
					while (!remoteCursorFlag.get(cursorHashKey) && !queryReqTimeout.get(cursorHashKey)) {
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					;
					if (queryReqTimeout.get(cursorHashKey)) {
						queryReqTimeout.remove(cursorHashKey);
						remoteCursorFlag.remove(cursorHashKey);
						continue;
					}
					if (remoteCursor.containsKey(cursorHashKey)) {
						final Cursor mergedCursor = new MergeCursor(new Cursor[]{
								tempCursor,
								remoteCursor.get(cursorHashKey)
						});
						remoteCursor.remove(cursorHashKey);
						remoteCursorFlag.remove(cursorHashKey);
						queryReqTimeout.remove(cursorHashKey);
						tempCursor = mergedCursor;
					}
				}
				Log.e(TAG, "Appended local and retrieved cursors from all other AVDs:[" + tempCursor.toString() + "]");
				queryCursor = tempCursor;
			} else {
				String destPort = getCoordinator(selection);
				if (destPort.equals(MYPORT)) {
					synchronized (InsertLock) {
						sqlDB = mySQLAccess.getReadableDatabase();
						Log.e(TAG, "Msg with key[" + selection + "] stored locally on port[" + MYPORT + "]");
						queryCursor = sqlDB.query(TABLE_NAME, queryProjection, "key = ?", queryColumnsVals,
								null, null, null);
					}
//				Log.e(TAG,"Query local message with key:["+selection+"], cursor:["+convertCursorToString(queryCursor)+"]");
				} else {
					String succPort1 = getSuccessor(destPort, 1);
					String succPort2 = getSuccessor(destPort, 2);
					if (succPort1.equals(MYPORT)) {
						synchronized (InsertLock) {
							sqlDB = mySQLAccess.getReadableDatabase();
							Log.e(TAG, "Msg with key[" + selection + "] not stored locally, but on [" + destPort + "], but I am 1st successor[" + succPort1 + "] will fetch from my DB");
							queryCursor = sqlDB.query(TABLE_NAME, queryProjection, "key = ?", queryColumnsVals,
									null, null, null);
						}
//					Log.e(TAG,"Query local message with key:["+selection+"], cursor:["+convertCursorToString(queryCursor)+"], I am 1st Succ");
					} else if (succPort2.equals(MYPORT)) {
						synchronized (InsertLock) {
							sqlDB = mySQLAccess.getReadableDatabase();
							Log.e(TAG, "Msg with key[" + selection + "] not stored locally, but on [" + destPort + "], but I am 2nd successor[" + succPort2 + "] will fetch from my DB");
							queryCursor = sqlDB.query(TABLE_NAME, queryProjection, "key = ?", queryColumnsVals,
									null, null, null);
						}
//					Log.e(TAG,"Query local message with key:["+selection+"], cursor:["+convertCursorToString(queryCursor)+"], I am 2nd Succ");
					} else {
						//TODO forward to successors also
						Log.e(TAG, "Msg with key[" + selection + "] not stored locally, but on [" + destPort + "], hence forwarding");
						String cursorHashKey = selection + "_" + destPort;
						queryReqTimeout.put(cursorHashKey, false);

						remoteCursorFlag.put(cursorHashKey, false);
						forwardQueryToNode(selection, destPort);

						while (!remoteCursorFlag.get(cursorHashKey) && !(queryReqTimeout.get(cursorHashKey))) {
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						;
						if (queryReqTimeout.get(cursorHashKey)) {
							remoteCursor.remove(cursorHashKey);
							queryReqTimeout.remove(cursorHashKey);
							destPort = succPort1;
							cursorHashKey = selection + "_" + destPort;
							queryReqTimeout.put(cursorHashKey, false);

							remoteCursorFlag.put(cursorHashKey, false);
							forwardQueryToNode(selection, destPort);
							while (!remoteCursorFlag.get(cursorHashKey) && !(queryReqTimeout.get(cursorHashKey))) {
								try {
									Thread.sleep(50);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
							;
							if (queryReqTimeout.get(cursorHashKey)) {
								remoteCursor.remove(cursorHashKey);
								queryReqTimeout.remove(cursorHashKey);
								destPort = succPort2;
								cursorHashKey = selection + "_" + destPort;
								queryReqTimeout.put(cursorHashKey, false);

								remoteCursorFlag.put(cursorHashKey, false);
								forwardQueryToNode(selection, destPort);
								while (!remoteCursorFlag.get(cursorHashKey) && !(queryReqTimeout.get(cursorHashKey))) {
									try {
										Thread.sleep(50);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
								;
								remoteCursor.remove(cursorHashKey);
								queryReqTimeout.remove(cursorHashKey);
							}


						}
						if (remoteCursor.containsKey(cursorHashKey)) {
							queryCursor = remoteCursor.get(cursorHashKey);
							remoteCursorFlag.remove(cursorHashKey);
						}
					}
				}
			}
		}
		return queryCursor;
	}

	private void forwardQueryToNode(String selection, String destNode) {
		String msg = QUERYMSG+ DELIM+ destNode+DELIM+MYPORT+DELIM+selection+DELIM+selection+"_"+destNode;
		Log.e(TAG,"Forwarding Query for key["+selection+"] to "+destNode+"as Msg:["+msg+"]");
		Thread fwdQueryThread = new Thread(new ClientTask(msg));
		fwdQueryThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

	}

	private void forwardUQueryToNode(String selection, String destNode) {
		String msg = UQUERYMSG+ DELIM+ destNode+DELIM+MYPORT+DELIM+selection+DELIM+selection+"_"+destNode;
		Log.e(TAG,"Forwarding Query for key["+selection+"] to "+destNode+"as Msg:["+msg+"]");
		Thread fwdQueryThread = new Thread(new ClientTask(msg));
		fwdQueryThread.start();
//		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

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

					if(msgType == INSERTMSG){
						ret = insertIntoMyDB(str);
						Log.e(TAG,"Received msg to insert into my DB, MSG["+str+"]");
					}else if(msgType == DELETEMSG){
						ret = forceDelete(str);
					}else if(msgType == QUERYMSG){
						ret = forceQuery(str,1);
						Log.e(TAG,"Received msg to query from my DB, MSG["+str+"], sending resp:["+ret+"]");
					}else if(msgType == QRESPMSG){
						ret = receiveQResp(str);
					}else if(msgType == UPDATEREQ){
						ret = sendRowsToUpdate(str);
					}else if(msgType == UPDATERESP){
						ret = receiveUResp(str);
					}else if(msgType == UQUERYMSG){
						ret = forceQuery(str,0);
					}

					PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
					ackSender.println(ret);

//                    publishProgress(str);
					br.close();
					cliSock.close();
				} catch (IOException e) {
					Log.e(TAG, "Error when reading and publishingProgress");
					e.printStackTrace();
				}
			}

			return null;

		}

		private String sendRowsToUpdate(String str) {
			Thread sendUpdateThread = new Thread(new CollectRows(str));
			sendUpdateThread.start();
			return ACKSTR+"";
		}

		private String receiveQResp(String str) {
			Thread queryRespProcessor = new Thread(new ProcessQueryResponse(str));
			queryRespProcessor.start();
			return ACKSTR+"";

		}

		private String receiveUResp(String str) {
			Thread updateRespProcessor = new Thread(new ProcessUpdateResponse(str));
			updateRespProcessor.start();
			return ACKSTR+"";

		}

		private String forceQuery(String str, int mode) {

			Thread queryThread = new Thread(new RunQuery(str,mode));
			queryThread.start();
			return ACKSTR+"";
		}

		private String forceDelete(String str) {
			Thread deleteThread = new Thread(new RemoteDelete(str));
			deleteThread.start();
			return ACKSTR+"";
		}

		private String insertIntoMyDB(String str) {
			Thread insertThread = new Thread(new RemoteInsert(str));
			insertThread.start();
			return ACKSTR+"";
		}

		public class RemoteDelete implements Runnable{

			String cmd;
			public RemoteDelete(String msg){
				this.cmd = msg;
			}
			public void run(){
				String[] msgs = cmd.split(DELIM);
				SQLiteDatabase sqlDB;// = mySQLAccess.getWritableDatabase();

				if(msgs[2].equals("*")){
					synchronized (InsertLock) {
						Log.e(TAG, "Delete * :all local messages");
						sqlDB = mySQLAccess.getWritableDatabase();
						sqlDB.rawQuery("delete from " + TABLE_NAME, null);
					}
				}else{
					synchronized (InsertLock) {
						String[] keys = {msgs[2]};
						sqlDB = mySQLAccess.getWritableDatabase();
						sqlDB.delete(TABLE_NAME, "key = ?", keys);
					}
				}
			}

		}

		public class RemoteInsert implements Runnable{

			String cmd;
			public RemoteInsert(String msg){
				this.cmd = msg;
			}
			public void run(){
				ContentValues values = new ContentValues();
				String[] msgs = cmd.split(DELIM);
				String key =msgs[2];
				String val =msgs[3];
				String destPort = msgs[4];
				values.put(COLUMN_KEY,key);
				values.put(COLUMN_VAL,val);
				forceInsert(values,destPort);
			}

		}
		public class ProcessQueryResponse implements Runnable{

			String resp;
			public ProcessQueryResponse(String msg){
				this.resp = msg;
			}
			public void run() {
				String queryResStr[] = resp.split(DELIM);

				if (queryResStr.length > 3) {
					Cursor tempCursor = convertStringToCursor(queryResStr[3]);
					remoteCursor.put(queryResStr[2], tempCursor);
				}
				remoteCursorFlag.put(queryResStr[2], true);

			}

		}
		public class ProcessUpdateResponse implements Runnable{

			String resp;
			public ProcessUpdateResponse(String msg){
				this.resp = msg;
			}
			public void run() {
				String queryResStr[] = resp.split(DELIM);

				if (queryResStr.length > 4) {
					Cursor tempCursor = convertStringToCursorFromUpdate(queryResStr[4],queryResStr[2]);
					remoteCursor.put(queryResStr[3], tempCursor);
				}
				remoteCursorFlag.put(queryResStr[3], true);

			}

		}

		public class RunQuery implements Runnable{

			String req;
			int mode;
			public RunQuery(String str, int type){
				this.req = str;
				this.mode = type;
			}
			public void run(){
				String[] msgs = req.split(DELIM);
				String key = msgs[3];
				String cKey = msgs[4];
				Cursor tempCursor = null;

				if(key.equals("*")) {
					SQLiteDatabase sqlDB;// = mySQLAccess.getReadableDatabase();
					synchronized (InsertLock) {
						sqlDB = mySQLAccess.getReadableDatabase();
						tempCursor = sqlDB.rawQuery("select " + COLUMN_KEY + "," + COLUMN_VAL + " from " + TABLE_NAME, null);
					}
				}else {
					if(mode == 1) {
						tempCursor = query(mUri, null, key, null, null);
					}else{
						SQLiteDatabase sqlDB;// = mySQLAccess.getReadableDatabase();
						String[] queryProjection = {COLUMN_KEY,COLUMN_VAL};
						String[] queryColumnsVals = {key};
						synchronized (InsertLock) {
							sqlDB = mySQLAccess.getReadableDatabase();
							tempCursor = sqlDB.query(TABLE_NAME, queryProjection, "key = ?", queryColumnsVals,
									null, null, null);
						}
					}
				}
				String cursorStr = convertCursorToString(tempCursor);
				String newMsg = QRESPMSG+DELIM+msgs[2]+DELIM+cKey+DELIM+cursorStr;
				Thread runQThread = new Thread(new ClientTask(newMsg));
				runQThread.start();
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsg);

			}

		}

		public class CollectRows implements Runnable{

			String req;
			public CollectRows(String str){
				this.req = str;
			}
			public void run(){
				String[] msgs = req.split(DELIM);
				String key = msgs[3];
				String cKey = msgs[4];
				Cursor tempCursor = null;

					SQLiteDatabase sqlDB;// = mySQLAccess.getReadableDatabase();
//				synchronized (InsertLock) {
					sqlDB = mySQLAccess.getReadableDatabase();
					tempCursor = sqlDB.rawQuery("select " + COLUMN_KEY + "," + COLUMN_VAL + " from " + TABLE_NAME, null);
//				}
				String cursorStr = convertCursorToString(tempCursor);
				String newMsg = UPDATERESP+DELIM+msgs[2]+DELIM+MYPORT+DELIM+cKey+DELIM+cursorStr;
				Thread collectRowsThread = new Thread(new ClientTask(newMsg));
				collectRowsThread.start();
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMsg);

			}

		}



	}

	private String convertCursorToString(Cursor tempCursor) {
		String ret="";
		tempCursor.moveToFirst();
		while(tempCursor.isAfterLast()== false){
			ret = ret+tempCursor.getString(0)+CURSORDELIM+tempCursor.getString(1)+CURSORDELIM;;
//			ret = ret+tempCursor.getString(0)+CURSORDELIM+tempCursor.getString(1)+CURSORDELIM+tempCursor.getString(2)+CURSORDELIM+tempCursor.getString(3)+CURSORDELIM;;
			tempCursor.moveToNext();
		}
		return ret;
	}

	private Cursor convertStringToCursor(String queryResStr) {
//		MatrixCursor tempMC = new MatrixCursor(new String[] {COLUMN_KEY,COLUMN_VAL,COLUMN_OWNER,COLUMN_VERSION});
		MatrixCursor tempMC = new MatrixCursor(new String[] {COLUMN_KEY,COLUMN_VAL});
		String cursorElem[] = queryResStr.split(CURSORDELIM);
		/*for (int i = 0; i<cursorElem.length;i+=4 ){
			String array[] = new String[4];
			array[0] = cursorElem[i];
			array[1] = cursorElem[i+1];
			array[2] = cursorElem[i+2];
			array[3] = cursorElem[i+3];

			tempMC.addRow(array);

		}*/
		for (int i = 0; i<cursorElem.length;i+=2 ){
			String array[] = new String[2];
			array[0] = cursorElem[i];
			array[1] = cursorElem[i+1];


			tempMC.addRow(array);

		}
		return (Cursor)tempMC;
	}

	private Cursor convertStringToCursorFromUpdate(String queryResStr, String otherPort) {
		MatrixCursor tempMC = new MatrixCursor(new String[] {COLUMN_KEY,COLUMN_VAL});
		String cursorElem[] = queryResStr.split(CURSORDELIM);

		if(otherPort.equals(SUCCESSOR1PORT)||otherPort.equals(SUCCESSOR2PORT)){
			//get my keys
			for (int i = 0; i<cursorElem.length;i+=2 ){
				String array[] = new String[2];
				array[0] = cursorElem[i];
				array[1] = cursorElem[i+1];

				if(getCoordinator(array[0]).equals(MYPORT)) {
					tempMC.addRow(array);
				}

			}
		}else{
			//get pred keys
			for (int i = 0; i<cursorElem.length;i+=2 ){
				String array[] = new String[2];
				array[0] = cursorElem[i];
				array[1] = cursorElem[i+1];

				if(getCoordinator(array[0]).equals(PREDECESSOR1PORT)||getCoordinator(array[0]).equals(PREDECESSOR2PORT)) {
					tempMC.addRow(array);
				}
			}
		}


		return (Cursor)tempMC;
	}

	public class ClientTask implements Runnable{
		String msgToSend;
		public ClientTask(String str){
			msgToSend = str;
		}
		public void run(){
			msgToSend = msgToSend.replace("\n", "");
			String msgStrings[] = msgToSend.split(DELIM);
			int msgType = Integer.parseInt(msgStrings[0]);
			String ret ="";
			if(msgType == INSERTMSG||msgType==DELETEMSG||msgType==QRESPMSG||msgType==UPDATERESP){
				sendMsgToNode(msgToSend);
			}else if(msgType == QUERYMSG||msgType == UQUERYMSG){
				ret = sendMsgToNode(msgToSend);
				if(ret.equals("-1")){
					queryReqTimeout.put(msgStrings[4],true);
				}


			}else if(msgType == UPDATEREQ){
				ret = sendMsgToNode(msgToSend);
				if(ret.equals("-1")){
					queryReqTimeout.put(msgStrings[4],true);
				}


			}
		}
		private String sendMsgToNode(String msg) {
			try {
				String destPort = msg.split(DELIM)[1];
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(destPort));
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

				out.println(msg);
				Log.e(TAG,"Sending Msg to :"+destPort+" Msg:["+msg+"]");

				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String reply;
				long t1 = System.currentTimeMillis();
				do {
					reply = br.readLine();
					Thread.sleep(50);
					long t2 = System.currentTimeMillis();
					if (t2-t1>200){
						Log.e(TAG,"Client Req Timed out for message:["+msg+"]");
						return "-1";
					}
				}while (reply==null);
				socket.close();
				return reply;
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}
	}


//	private class ClientTask  extends AsyncTask<String, Void, Void> {
//		@Override
//		protected Void doInBackground(String... msgs) {
//
//			String msgToSend = msgs[0];
//			msgToSend = msgToSend.replace("\n", "");
//			String msgStrings[] = msgToSend.split(DELIM);
//			int msgType = Integer.parseInt(msgStrings[0]);
//			String ret ="";
//			if(msgType == INSERTMSG||msgType==DELETEMSG||msgType==QRESPMSG||msgType==UPDATERESP){
//				sendMsgToNode(msgToSend);
//			}else if(msgType == QUERYMSG){
//				ret = sendMsgToNode(msgToSend);
//				if(ret.equals("-1")){
//					queryReqTimeout.put(msgStrings[4],true);
//				}
//
//
//			}else if(msgType == UPDATEREQ){
//				ret = sendMsgToNode(msgToSend);
//				if(ret.equals("-1")){
//					queryReqTimeout.put(msgStrings[4],true);
//				}
//
//
//			}
//			return null;
//		}
//
//		private String sendMsgToNode(String msg) {
//			try {
//				String destPort = msg.split(DELIM)[1];
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(destPort));
//				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
//
//				out.println(msg);
//				Log.e(TAG,"Sending Msg to :"+destPort+" Msg:["+msg+"]");
//
//				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//				String reply;
//                long t1 = System.currentTimeMillis();
//				do {
//					reply = br.readLine();
//					Thread.sleep(50);
//                    long t2 = System.currentTimeMillis();
//                    if (t2-t1>200){
//						Log.e(TAG,"Client Req Timed out for message:["+msg+"]");
//                        return "-1";
//                    }
//				}while (reply==null);
//				socket.close();
//				return reply;
//			} catch (UnknownHostException e) {
//				Log.e(TAG, "ClientTask UnknownHostException");
//			} catch (IOException e) {
//				Log.e(TAG, "ClientTask socket IOException");
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			return null;
//		}
//
//	}
}