package edu.buffalo.cse.cse486586.groupmessenger1;

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
import java.net.UnknownHostException;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String ACK_STRING = "PA-02-A OK";
    static final String REMOTE_PORTS[] = {"11108","11112","11116","11120","11124"};
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
//my changes
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        //changes end

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
        findViewById(R.id.button1).setOnClickListener(new OnPTestClickListener(tv, getContentResolver()));

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
                String msg = editText.getText().toString() + "\n";
                tv1.append("Sending:"+msg+"\n");
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

        private ContentResolver mContentResolver = getContentResolver();
        private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
        private ContentValues mContentValues =  new ContentValues();


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
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


                    PrintWriter ackSender = new PrintWriter(cliSock.getOutputStream(), true);
                    ackSender.println(ACK_STRING);
//write to DB
                    mContentValues.put(KEY_FIELD, Integer.toString(msgKey));
                    mContentValues.put(VALUE_FIELD, str);
                    try {
                        mContentResolver.insert(mUri, mContentValues);
                        msgKey++;
                    } catch (Exception e) {
                        Log.e(TAG, e.toString());
                        return null;
                    }
//                        publishProgress(str);


                    br.close();
                    cliSock.close();
                } catch (IOException e) {
                    Log.e(TAG,"Error when reading and publishingProgress");
                    e.printStackTrace();
                }
            }


            return null;
        }

    }



    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            msgToSend = msgToSend.replace("\n", "");

            for(int i=0;i<5;i++){
                sendMessageToRemote(REMOTE_PORTS[i], msgToSend);
            }

            return null;
        }

        private Void sendMessageToRemote(String remotePort, String msgToSend) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                out.println(msgToSend);

                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                while (!(br.readLine().contentEquals(ACK_STRING))) ;
//                while (br.readLine()==null) ;
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

}