package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by prashanth on 5/7/17.
 */

public class MySQLiteOpenHelper extends SQLiteOpenHelper {

    private static final String DATABASE_NAME = "msgKeyVal.db";
    private static final int DATABASE_VERSION = 1;

    public static final String TABLE_NAME = "msgKeyVal";

    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VAL = "value";
    public static final String COLUMN_OWNER = "owner";
    public static final String COLUMN_VERSION = "version";



    static final String DATABASE_CREATE = "create table " + TABLE_NAME + "(" + COLUMN_KEY + " text unique, " + COLUMN_VAL + " text" + ");";
//    static final String DATABASE_CREATE = "create table " + TABLE_NAME + "(" + COLUMN_KEY + " text, " + COLUMN_VAL + " text, " + COLUMN_OWNER + " text, " + COLUMN_VERSION + " text" + ");";

    public MySQLiteOpenHelper(Context context){
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public  void onCreate(SQLiteDatabase database) {
        database.execSQL(DATABASE_CREATE);
    }

    @Override
    public  void onUpgrade(SQLiteDatabase database, int oldVersion, int newVersion) {
        database.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(database);
    }

}
