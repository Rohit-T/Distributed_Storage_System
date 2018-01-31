package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static String[] avds = new String[] {"5554", "5556", "5558", "5560", "5562"};
	static HashMap<String, String> nodeToPort = new HashMap<String, String>();
	static HashMap<String, ValueVersionMap> LatestVer = new HashMap<String, ValueVersionMap>();
	static LinkedList<String> nodes;
	static int nodesLen = -1;
	static int myindex = -1;
	static String mynode_id = null;
	static String myPort = null;
	static String[] mySuccessor = null;
	static String[] myPredecessor = null;
	static boolean recovered = false;
	static boolean deletion = false;
	static boolean loaded = false;

	GMDbHelper dbHelper;
	static String DatabaseName = "SimpleDynamo.db";
	static int DatabaseVersion = 1;
	static String Tablename = "messages";

	private final ReentrantLock lock = new ReentrantLock();
	RecoveryTask RT;

	Uri gmUri;

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public class GMDbHelper extends SQLiteOpenHelper {

		public GMDbHelper(Context context) {
			super(context, DatabaseName, null, DatabaseVersion);
		}

		public void onCreate(SQLiteDatabase GMdb) {
			GMdb.execSQL("CREATE TABLE " + Tablename +"(_id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER, key TEXT UNIQUE, value TEXT)");
		}

		public void onUpgrade(SQLiteDatabase GMdb, int OldVersion, int NewVersion) {
			GMdb.execSQL("DROP TABLE IF EXISTS" + Tablename);
			onCreate(GMdb);
		}

	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		if(selection.equals("@")){

			SQLiteDatabase db = dbHelper.getReadableDatabase();

			int returncode = db.delete(Tablename, null, null);

		}else if(selection.equals("*")) {

			Log.e("Delete *", "PLEASE CHECK");

		}else {
			int index = findCoordinator(selection);

			if(myindex == index){
				SQLiteDatabase db = dbHelper.getReadableDatabase();

				String[] condition = { selection };

				int returncode = db.delete(Tablename, "key = ?", condition);

				if(!deletion) {

					String ret = ClientCase5(selection, mySuccessor[0], mySuccessor[1]);

				}

			}else if(myPredecessor[0].equals(nodes.get(index)) || myPredecessor[1].equals(nodes.get(index))) {
				if(deletion) {

					SQLiteDatabase db = dbHelper.getReadableDatabase();

					String[] condition = { selection };

					int returncode = db.delete(Tablename, "key = ?", condition);

				}else {

					SQLiteDatabase db = dbHelper.getReadableDatabase();

					String[] condition = { selection };

					int returncode = db.delete(Tablename, "key = ?", condition);

					if(myPredecessor[0].equals(nodes.get(index))){

						String ret = ClientCase5(selection, myPredecessor[0], mySuccessor[0]);

					}else if(myPredecessor[1].equals(nodes.get(index))){

						String ret = ClientCase5(selection, myPredecessor[1], myPredecessor[0]);

					}

				}
			}else {

				String ret = ClientCase6(selection, nodes.get(index));

			}

		}
		deletion = false;
		return 0;
	}

	public String ClientCase5(String key, String node1, String node2) {
		String message = 2 + "|~" + key;
		sendMessage(message, node1);
		return sendMessage(message, node2);
	}

	public String ClientCase6(String key, String coordinator) {

		String[] successor = new String[2];
		String message = 2 + "|~" + key;
		String result = sendMessage(message, coordinator);
		for (int i = 0; i < 2; i++) {
			successor[i] = nodes.get((nodes.indexOf(coordinator) + (i + 1)) % nodesLen);
			result = sendMessage(message, successor[i]);
		}
		return result;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		int index;
		synchronized (RT) {
			while(!recovered) {
				try{
					wait();
				}catch(InterruptedException IE) {
					Log.e("Inside Insert", " Exception while waiting");
				}
			}
		}

		String key = values.getAsString("key");
		String value = values.getAsString("value");

		index = findCoordinator(key);

		if(myindex == index) {

			insertKey(key, value);

			String ret = ClientCase0(key, value, mySuccessor[0], mySuccessor[1]);

		}else if(myPredecessor[0].equals(nodes.get(index)) || myPredecessor[1].equals(nodes.get(index))) {

			insertKey(key, value);

			if(myPredecessor[0].equals(nodes.get(index))){

				String ret = ClientCase0(key, value, myPredecessor[0], mySuccessor[0]);

			}else if(myPredecessor[1].equals(nodes.get(index))){

				String ret = ClientCase0(key, value, myPredecessor[0], myPredecessor[1]);

			}

		}else {

			String ret = ClientCase1(key, value, nodes.get(index));

		}

		return uri;
	}

	public Uri serverinsert(Uri uri, ContentValues values) {

		lock.lock();
		String key = values.getAsString("key");
		String value = values.getAsString("value");

		insertKey(key, value);
		lock.unlock();

		return uri;
	}

	public void insertKey(String key, String value){

		Cursor dbcursor;
		int newVersion;

		SQLiteDatabase db = dbHelper.getReadableDatabase();

		String[] condition = { key };

		dbcursor = db.query(Tablename, null, "key = ? ", condition, null, null, null);

		if(dbcursor.getCount() > 0) {
			int retVersion;
			int versionIndex = dbcursor.getColumnIndex("version");

			dbcursor.moveToNext();
			retVersion = dbcursor.getInt(versionIndex);

			newVersion = retVersion + 1;

			SQLiteDatabase sqldb = dbHelper.getWritableDatabase();

			ContentValues newvalues = new ContentValues();
			newvalues.put("key", key);
			newvalues.put("value", value);
			newvalues.put("version", newVersion);

			int UpdatedRows = sqldb.update(Tablename, newvalues, "key = ?", condition);

		}else {
			SQLiteDatabase sqldb = dbHelper.getWritableDatabase();

			ContentValues newvalues = new ContentValues();
			newvalues.put("key", key);
			newvalues.put("value", value);
			newvalues.put("version", 1);

			long RowId = sqldb.insert(Tablename, null, newvalues);

		}

		try {
			dbcursor.close();
		}catch(NullPointerException NPE) {
			Log.e("Inside Insert", "Unable to close the cursor object");
		}
	}

	public String sendMessage(String message, String node) {

		Socket socket = null;
		String input = null;
		DataInputStream dis = null;
		DataOutputStream dos = null;

		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeToPort.get(node)));

		}catch(UnknownHostException UHE) {
			Log.e("Inside Sendmessage", "Unable to create socket");
		}catch(IOException IOE) {
			Log.e("inside sendmessage", "Input Output Exception while creating socket for " + node);
		}

		try {
			dos = new DataOutputStream(socket.getOutputStream());
			dos.writeUTF(message);
			dos.flush();
		} catch (IOException IOE) {
			Log.e("Inside sendmesage", "IOE Client: Print Stream Error");
		} catch(NullPointerException NPE){
			Log.e("Inside sendmessage", "NPE while sending request");
		}

		try {
			dis = new DataInputStream(socket.getInputStream());
		} catch (IOException IOE) {
			Log.e("Inside sendmessage", "IOE Client: Unable to read from Input Stream");
		} catch (NullPointerException NPE) {
			Log.e("Inside sendmessage", "NPE Client: while reading from Input Stream");
		}

		try {
			input = dis.readUTF();
		} catch (IOException IOE) {
			Log.e("inside sendmessage", "IOE Client: Unable to read maybe socket timeout for " + node);

			try{
				dos.close();
				dis.close();
				socket.close();
			}catch (IOException IOE1){
				Log.e("inside sendmessage", "IOE  Client: unable to release resources");
			}catch (NullPointerException NPE) {
				Log.e("Inside sendmessage", "Client: NPE while releasing resources in failure");
			}
			return "FAILED";
		} catch (NullPointerException NPE) {
			Log.e("inside sendmessage", "NPE Client: while reading");
		}

		try{
			dos.close();
			dis.close();
			socket.close();
		}catch (IOException IOE){
			Log.e("Inside sendmessage", "IOE  Client: unable to release resources");
		}catch (NullPointerException NPE) {
			Log.e("Inside sendmessage", "Client: NPE while releaseing resources");
		}

		return input;
	}

	public String ClientCase0 (String key, String value, String node1, String node2) {
		String message = 0 + "|~" + key + "|~" + value;
		sendMessage(message, node1);
		return sendMessage(message, node2);
	}

	public String ClientCase1 (String key, String value, String coordinator) {
		String message = 0 + "|~" + key + "|~" + value;
		String result = sendMessage(message, coordinator);
		String[] successor = new String[2];
		for (int i = 0; i < 2; i++) {
			successor[i] = nodes.get((nodes.indexOf(coordinator) + (i + 1)) % nodesLen);
			result = sendMessage(message, successor[i]);
		}
		return result;
	}

	public int findCoordinator(String key) {

		for (int i = 0; i < nodes.size(); i++) {
			try {
				if(genHash(key).compareTo(genHash(nodes.get(i))) < 0){
					return i;
				}
			}catch(NoSuchAlgorithmException NSAE) {
				Log.e("Inside FindCoordinator", "unable to genhash");
			}
		}
		return 0;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		if(!loaded) {

			dbHelper = new GMDbHelper(getContext());
			gmUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

			mySuccessor = new String[2];
			myPredecessor = new String[2];

			nodes = new LinkedList<String>(Arrays.asList(avds));
			nodesLen = nodes.size();

			Collections.sort(nodes, new Comparator<String>() {
				public int compare(String avd1, String avd2) {
					try {
						return genHash(avd1).compareTo(genHash(avd2)) < 0 ? -1 : genHash(avd1).compareTo(genHash(avd2)) > 0 ? 1 : 0;
					}catch(NoSuchAlgorithmException NSAE) {
						Log.e("Inside onCreate", "genhash failed while sorting nodes");
						System.exit(1);
						return 0;
					}
				}
			} );

			try {
				TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
				myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
				//myPort = String.valueOf((Integer.parseInt(portStr) * 2));
				mynode_id = this.genHash(myPort);
			}catch(NoSuchAlgorithmException NSAE) {
				Log.e("Inside oncreate", "Unable to generate SHA-1 hash during finding myport");
			}catch(NullPointerException NPE) {
				Log.e("Inside oncreate", "Null pointer Exception while getting port");
			}

			for(int i = 0; i < 2 ; i++) {

				mySuccessor[i] = nodes.get((nodes.indexOf(myPort) + (i + 1)) % nodesLen);

			}

			for(int i = 0; i < 2 ; i++){

				myPredecessor[i] = nodes.get((nodesLen + (nodes.indexOf(myPort) - (i + 1))) % nodesLen);

			}

			myindex = nodes.indexOf(myPort);

			nodeToPort.put("5554", "11108");
			nodeToPort.put("5556", "11112");
			nodeToPort.put("5558", "11116");
			nodeToPort.put("5560", "11120");
			nodeToPort.put("5562", "11124");

			lock.lock();
			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			} catch (IOException e) {
				Log.e("Inside oncreate", "Can't create a ServerSocket");
			}

			try {
				RT = new RecoveryTask();

				String status = RT.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();

			}catch(InterruptedException IE) {
				Log.v("Inside Oncreate", "Interruted Exception");
			}catch(ExecutionException EE) {
				Log.v("Inside Oncreate", "Execution exception");
			}

			lock.unlock();
		}
		loaded = true;
		return true;
	}

	private class RecoveryTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {

			synchronized (this) {

				String[] recoveryValues = new String[4];
				recoveryValues[0] = sendMessage("1|~@", myPredecessor[1]);
				recoveryValues[1] = sendMessage("1|~@", myPredecessor[0]);
				recoveryValues[2] = sendMessage("1|~@", mySuccessor[0]);
				recoveryValues[3] = sendMessage("1|~@", mySuccessor[1]);

				for (int i = 0; i < 4; i++) {
					if(!recoveryValues[i].equals("FAILED")) {
						String[] pairs = recoveryValues[i].split("\\|~");

						for (int j = 0; j < pairs.length; j++) {
							String[] keyval = pairs[j].split("~~");
							int index = findCoordinator(keyval[1]);
							if(index == myindex || index == nodes.indexOf(myPredecessor[0]) || index == nodes.indexOf(myPredecessor[1])) {
								UpdateKeyVal(keyval[0], keyval[1], keyval[2]);
							}
						}
					}
				}
				recovered = true;

				notifyAll();
			}

			return "RecoveryDone";
		}

		public void UpdateKeyVal(String version, String key, String value){

			Cursor dbcursor;
			int newVersion;

			SQLiteDatabase db = dbHelper.getReadableDatabase();

			String[] condition = { key };

			dbcursor = db.query(Tablename, null, "key = ? ", condition, null, null, null);

			if(dbcursor.getCount() > 0) {
				int retVersion;
				int versionIndex = dbcursor.getColumnIndex("version");

				dbcursor.moveToNext();
				retVersion = dbcursor.getInt(versionIndex);

				if(retVersion < Integer.parseInt(version)) {

					SQLiteDatabase sqldb = dbHelper.getWritableDatabase();

					ContentValues newvalues = new ContentValues();
					newvalues.put("key", key);
					newvalues.put("value", value);
					newvalues.put("version", version);

					int UpdatedRows = sqldb.update(Tablename, newvalues, "key = ?", condition);

				}

			}else {
				SQLiteDatabase sqldb = dbHelper.getWritableDatabase();

				ContentValues newvalues = new ContentValues();
				newvalues.put("key", key);
				newvalues.put("value", value);
				newvalues.put("version", version);

				long RowId = sqldb.insert(Tablename, null, newvalues);

			}

			try {
				dbcursor.close();
			}catch(NullPointerException NPE) {
				Log.e("Inside Insert", "Unable to close the cursor object");
			}
		}

		public String sendMessage(String message, String node) {

			Socket socket = null;
			String input = null;
			DataInputStream dis = null;
			DataOutputStream dos = null;

			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeToPort.get(node)));
			}catch(UnknownHostException UHE) {
				Log.e("Inside Sendmessage", "Unable to create socket");
			}catch(IOException IOE) {
				Log.e("inside sendmessage", "Input Output Exception while creating socket for " + node);
			}

			try {
				dos = new DataOutputStream(socket.getOutputStream());
				dos.writeUTF(message);
				dos.flush();
			} catch (IOException IOE) {
				Log.e("Inside sendmesage", "IOE Client: Print Stream Error");
			} catch(NullPointerException NPE){
				Log.e("Inside sendmessage", "NPE while sending request");
			}

			try {
				dis = new DataInputStream(socket.getInputStream());
			} catch (IOException IOE) {
				Log.e("Inside sendmessage", "IOE Client: Unable to read from Input Stream");
			} catch (NullPointerException NPE) {
				Log.e("Inside sendmessage", "NPE Client: while reading from Input Stream");
			}

			try {
				input = dis.readUTF();
			} catch (IOException IOE) {
				Log.e("inside sendmessage", "IOE Client: Unable to read maybe socket timeout for " + node);

				try{
					dos.close();
					dis.close();
					socket.close();
				}catch (IOException IOE1){
					Log.e("inside sendmessage", "IOE  Client: unable to release resources");
				}catch (NullPointerException NPE) {
					Log.e("Inside sendmessage", "Client: NPE while releasing resources in failure");
				}
				return "FAILED";
			} catch (NullPointerException NPE) {
				Log.e("inside sendmessage", "NPE Client: while reading");
			}

			try{
				dos.close();
				dis.close();
				socket.close();
			}catch (IOException IOE){
				Log.e("Inside sendmessage", "IOE  Client: unable to release resources");
			}catch (NullPointerException NPE) {
				Log.e("Inside sendmessage", "Client: NPE while releaseing resources");
			}

			return input;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		synchronized (RT) {
			while(!recovered) {
				try{
					wait();
				}catch(InterruptedException IE) {
					Log.v("Inside Insert", " Exception while waiting");
				}
			}
		}

		Cursor dbcursor = null;

		if(selection.equals("@")){

			SQLiteDatabase db = dbHelper.getReadableDatabase();

			dbcursor = db.query(Tablename, null, null, null, null, null, null);

		}else if(selection.equals("*")) {
			int counter = 0;
			String keyvalver;

			SQLiteDatabase db = dbHelper.getReadableDatabase();

			dbcursor = db.query(Tablename, null, null, null, null, null, null);

			int keyIndex = dbcursor.getColumnIndex("key");
			int valueIndex = dbcursor.getColumnIndex("value");
			int versionIndex = dbcursor.getColumnIndex("version");

			dbcursor.moveToFirst();
			keyvalver = dbcursor.getString(versionIndex) + "~~" + dbcursor.getString(keyIndex) + "~~" + dbcursor.getString(valueIndex);

			while (dbcursor.moveToNext()) {
				keyvalver = keyvalver + "|~" + dbcursor.getString(versionIndex) + "~~" + dbcursor.getString(keyIndex) + "~~" + dbcursor.getString(valueIndex);
			}

			MergeQueryResult(keyvalver);

			while(counter < nodes.size()) {
				if(!myPort.equals(nodes.get(counter))){
					String msg = "@";

					String cursorAsString = ClientCase2(msg, nodes.get(counter));
					if(!cursorAsString.equals("FAILED")) {
						MergeQueryResult(cursorAsString);
					}
				}
				counter++;
			}

			MatrixCursor tempcursor = new MatrixCursor(new String[] { "key", "value" });

			for (Map.Entry<String, ValueVersionMap> entry : LatestVer.entrySet()) {

				String key = entry.getKey();
				ValueVersionMap value = entry.getValue();

				tempcursor.addRow(new String[] { key, value.val });

			}
			dbcursor = tempcursor;
			LatestVer.clear();
		}else {
			int index = findCoordinator(selection);

			if(myindex == index) {

				SQLiteDatabase db = dbHelper.getReadableDatabase();

				String[] condition = { selection };

				dbcursor = db.query(Tablename, null, "key = ? ", condition, null, null, null);

				Cursor tmp1, tmp2;

				tmp1 = ClientCase3(selection, mySuccessor[0]);
				tmp2 = ClientCase3(selection, mySuccessor[1]);

				if(tmp1 != null && tmp1.getCount() > 0) {
					dbcursor = compareCursor(dbcursor, tmp1);
				}
				if(tmp2 != null && tmp2.getCount() > 0) {
					dbcursor = compareCursor(dbcursor, tmp2);
				}

			}else if(myPredecessor[0].equals(nodes.get(index)) || myPredecessor[1].equals(nodes.get(index))) {
				SQLiteDatabase db = dbHelper.getReadableDatabase();

				String[] condition = { selection };

				dbcursor = db.query(Tablename, null, "key = ? ", condition, null, null, null);

				if(myPredecessor[0].equals(nodes.get(index))){
					Cursor tmp1, tmp2;

					tmp1 = ClientCase3(selection, myPredecessor[0]);
					tmp2 = ClientCase3(selection, mySuccessor[0]);

					if(tmp1 != null && tmp1.getCount() > 0) {
						dbcursor = compareCursor(dbcursor, tmp1);
					}
					if(tmp2 != null && tmp2.getCount() > 0) {
						dbcursor = compareCursor(dbcursor, tmp2);
					}
				}else if(myPredecessor[0].equals(nodes.get(index))) {
					Cursor tmp1, tmp2;

					tmp1 = ClientCase3(selection, myPredecessor[0]);
					tmp2 = ClientCase3(selection, myPredecessor[1]);

					if(tmp1 != null && tmp1.getCount() > 0) {
						dbcursor = compareCursor(dbcursor, tmp1);
					}
					if(tmp2 != null && tmp2.getCount() > 0) {
						dbcursor = compareCursor(dbcursor, tmp2);
					}
				}
			}else {

				dbcursor = ClientCase4(selection, nodes.get(index));

			}
		}

		return dbcursor;
	}

	public Cursor serverquery(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		Cursor dbcursor;

		SQLiteDatabase db = dbHelper.getReadableDatabase();

		if(selection.equals("@")) {

			dbcursor = db.query(Tablename, null, null, null, null, null, null);

		}else {

			String[] condition = {selection};
			lock.lock();
			dbcursor = db.query(Tablename, null, "key = ? ", condition, null, null, null);
			lock.unlock();
		}
		return dbcursor;
	}

	public Cursor compareCursor(Cursor a, Cursor b){

		int retVersion1;
		int retVersion2;

		int valueIndex1 = a.getColumnIndex("version");

		a.moveToFirst();

		retVersion1 = a.getInt(valueIndex1);

		int valueIndex2 = b.getColumnIndex("version");

		b.moveToFirst();

		retVersion2 = b.getInt(valueIndex2);

		if(retVersion1 > retVersion2) {
			return a;
		}else {
			return b;
		}
	}

	public void MergeQueryResult(String cursorString) {

		String[] inputMsg = null;

		try {
			inputMsg = cursorString.split("\\|~");
		} catch(NullPointerException NPE) {
			Log.e("Inside MergeQueryResult", " while splitting CursorString");
		}

		for(int i = 0 ; i < inputMsg.length; i++) {

			String temp[] = inputMsg[i].split("~~");

			if(LatestVer.get(temp[1]) != null) {
				if(LatestVer.get(temp[1]).ver < Integer.parseInt(temp[0])) {
					ValueVersionMap valvermap = new ValueVersionMap(temp[2], Integer.parseInt(temp[0]));
					LatestVer.put(temp[1], valvermap);
				}
			}else {
				ValueVersionMap valvermap = new ValueVersionMap(temp[2], Integer.parseInt(temp[0]));
				LatestVer.put(temp[1], valvermap);
			}

		}
	}

	public String ClientCase2(String key, String node) {
		String message = 1 + "|~" + key;
		return sendMessage(message, node);
	}

	public Cursor ClientCase3(String key, String node) {
		String[] inputMsg;
		MatrixCursor tempcursor = null;
		String message = 1 + "|~" + key;
		String result = sendMessage(message, node);
		if (!result.equals("FAILED")) {
			tempcursor = new MatrixCursor(new String[]{"version", "key", "value"});

			try {
				inputMsg = result.split("\\|~");
				for (int i = 0; i < inputMsg.length; i++) {
					String temp[] = inputMsg[i].split("~~");
					tempcursor.addRow(new String[]{temp[0], temp[1], temp[2]});
				}
			} catch (NullPointerException NPE) {
				Log.e("Inside client case 3", "unable to split");
			}
		}
		return tempcursor;
	}

	public Cursor ClientCase4(String key, String coordinator) {
		String[] inputMsgs;
		String[] result1 = new String[3];
		String[] successor = new String[2];
		String result = null;
		MatrixCursor tmpcursor = null;
		String message = 1 + "|~" + key;
		result1[2] = sendMessage(message, coordinator);
		for (int i = 0; i < 2; i++) {
			int loc = (nodes.indexOf(coordinator) + (i + 1)) % nodesLen;
			successor[i] = nodes.get(loc);
			try {
				result1[i] = sendMessage(message, successor[i]);
			}catch(NullPointerException NPE) {
				Log.e("Inside client case 4", "successor has failed");
			}
		}

		if(!result1[0].equals("FAILED") && !result1[1].equals("FAILED")) {
			if(Integer.parseInt(result1[0].split("~~")[0]) > Integer.parseInt(result1[0].split("~~")[0])) {
				result = result1[0];
			}else {
				result = result1[1];
			}
		}else if(!result1[0].equals("FAILED")) {
			result = result1[0];
		}else if(!result1[1].equals("FAILED")){
			result = result1[1];
		}

		if(!result1[2].equals("FAILED")) {
			if (Integer.parseInt(result1[2].split("~~")[0]) > Integer.parseInt(result.split("~~")[0])) {
				result = result1[2];
			}
		}

		if(!result.equals("FAILED")) {
			tmpcursor = new MatrixCursor(new String[]{"key", "value"});

			try {
				inputMsgs = result.split("\\|~");
				for (int i = 0; i < inputMsgs.length; i++) {
					String temp[] = inputMsgs[i].split("~~");
					tmpcursor.addRow(new String[]{temp[1], temp[2]});
				}
			} catch (NullPointerException NPE) {
				Log.e("Inside client case 4", "unable to split");
			}
		}
		return tmpcursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
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

	private class ValueVersionMap {
		String val;
		int ver;

		ValueVersionMap(String val, int ver) {
			this.val = val;
			this.ver = ver;
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket server = null;
			DataInputStream dis = null;
			String input = null;
			String inputMsg[] = null;
			DataOutputStream dos = null;
			String output = null;

			while(true) {
				try {
					server = serverSocket.accept();
				} catch (IOException IOE) {
					Log.e(TAG, "IOE Server: Socket unable to accept request");
				}

				try {
					dis = new DataInputStream(server.getInputStream());
				} catch (IOException IOE) {
					Log.e(TAG, "IOE Server: Unable to read from Input Stream");
				} catch (NullPointerException NPE) {
					Log.e(TAG, "NPE Server: while reading from input stream");
				}

				try {
					input = dis.readUTF();
				} catch (IOException IOE) {
					Log.e(TAG, "IOE Server: Unable to read from BufferedReader");
				} catch (NullPointerException NPE) {
					Log.e(TAG, "NPE Server: while reading line from buffered reader");
				}

				try {
					inputMsg = input.split("\\|~");
				} catch (NullPointerException NPE) {
					Log.e(TAG, "NPE Server: while splitting the input message");
				}

				switch (Integer.parseInt(inputMsg[0])) {
					case 0 :

						ContentValues cv = new ContentValues();
						cv.put("key", inputMsg[1]);
						cv.put("value", inputMsg[2]);

						Uri temp = serverinsert(gmUri, cv);

						try {
							dos = new DataOutputStream(server.getOutputStream());
							dos.writeUTF("ACK");
							dos.flush();
						} catch (IOException IOE) {
							Log.v(TAG, "IOE Server: Output Stream Error case 0");
						}

						break;
					case 1:
						String keyvalver = "FAILED";
						Cursor tmpCursor = serverquery(gmUri, null, inputMsg[1], null, null);

						int keyIndex = tmpCursor.getColumnIndex("key");
						int valueIndex = tmpCursor.getColumnIndex("value");
						int versionIndex = tmpCursor.getColumnIndex("version");

						if(tmpCursor.moveToFirst()) {
							keyvalver = tmpCursor.getString(versionIndex) + "~~" + tmpCursor.getString(keyIndex) + "~~" + tmpCursor.getString(valueIndex);
						}

						while (tmpCursor.moveToNext()) {
							keyvalver = keyvalver + "|~" + tmpCursor.getString(versionIndex) + "~~" + tmpCursor.getString(keyIndex) + "~~" + tmpCursor.getString(valueIndex);
						}

						try {
							dos = new DataOutputStream(server.getOutputStream());
							dos.writeUTF(keyvalver);
							dos.flush();
						} catch (IOException IOE) {
							Log.v(TAG, "IOE Server: Output Stream Error case 0");
						}
						break;
					case 2 :
						deletion = true;
						int returncode = delete(gmUri,inputMsg[1],null);

						try {
							dos = new DataOutputStream(server.getOutputStream());
							dos.writeUTF("ACK");
							dos.flush();
						} catch (IOException IOE) {
							Log.v(TAG, "IOE Server: Output Stream Error case 0");
						}

						break;
					default : Log.e("Inside Server", "Invalid case");
				}
			}
		}
	}
}
