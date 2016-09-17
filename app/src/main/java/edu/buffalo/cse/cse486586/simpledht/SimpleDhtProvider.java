package edu.buffalo.cse.cse486586.simpledht;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static String predecessor="0";
    static String successor="0";
    static String Myport="";
    static int count=0;
    final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    Comparator<NodeObject> NodeCompare = new NodeComparator();
    BlockingQueue<NodeObject> pq= new PriorityBlockingQueue<NodeObject>(100, NodeCompare);
    BlockingQueue<NodeObject> pq1= new PriorityBlockingQueue<NodeObject>(100, NodeCompare);


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    public class NodeObject {
        String portnum;
        String hashvalue;
        String pre;
        String suc;

        public NodeObject() {
        }

        public NodeObject(String port, String gh) {
            this.portnum=port;
            this.hashvalue=gh;
            this.pre=null;
            this.suc=null;
        }
    }


    public class NodeComparator implements Comparator<NodeObject>
    {
        @Override
        public int compare(NodeObject n1, NodeObject n2)
        {
            if (n1.hashvalue.compareTo(n2.hashvalue)<0)
            { return -1; }
            if (n1.hashvalue.compareTo(n2.hashvalue)>0)
            { return 1;  }
            return 0;
        }
    }


    @Override
    public boolean onCreate() {
        Log.e("Oncreate", "got in");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Myport=myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 100);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        if (!Myport.equals("11108"))
        {
            Log.e("Myport", Myport);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            new ClientTaskNJ().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
        }
        if (Myport.equals("11108"))
        {
            for (int i=0; i<5; i++)
            {
                Log.e("Myport NodeObject", REMOTE_PORT[i]);
                try {
                    String emuhash=String.valueOf(Integer.parseInt(REMOTE_PORT[i])/2);
                    NodeObject n= new NodeObject(REMOTE_PORT[i],genHash(emuhash));
                    pq.add(n);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }


    public class Addnode {
        public NodeObject addnew (String newnode)
        {
            Log.e("addnew",newnode);
            NodeObject current=new NodeObject();
            NodeObject start=new NodeObject();
            NodeObject next=new NodeObject();
            NodeObject prev=new NodeObject();
            if (count==0)
            {
                count++;
                while(pq.size()>0) {
                    NodeObject newn=pq.poll();
                    if (newn.portnum.equals("11108"))
                    {
                        newn.pre=newnode;
                        newn.suc=newnode;
                        predecessor =newnode;
                        successor=newnode;
                        pq1.add(newn);
                    }
                    else if (newn.portnum.equals(newnode))
                    {
                        newn.pre="11108";
                        newn.suc="11108";
                        current=newn;
                        Log.e("current",current.pre);
                        pq1.add(newn);
                    }
                    else
                    {
                        pq1.add(newn);
                    }
                }
                pq1.drainTo(pq);
            }

            else {
                int cur=0; int sta=0; int nex=0; int pre=0;
                while(pq.size()>0) {
                    NodeObject newn=pq.poll();
                    if (newn.portnum.equals(newnode))
                    {
                        current=newn;
                        if (prev.portnum!=null)
                        {
                            pre=1;
                        }
                        cur=1;
                    }
                    else if(sta==0 && cur==0 && newn.pre!=null && newn.suc!=null)
                    {
                        start=newn;
                        prev=newn;
                        count=2;
                        sta=1;
                    }
                    else if (cur==0 && pre==0 && newn.pre!=null && newn.suc!=null)
                    {
                        if (prev.portnum==null) {
                            prev = newn;
                        }
                        else if (prev.portnum!=null) {
                            if (count==2)
                            {
                                prev=newn;
                                count=1;
                            }
                            else {
                                pq1.add(prev);
                                Log.e("pq1 add", newn.portnum);
                                prev = newn;
                            }
                        }
                    }
                    else if (cur==1 && nex==0 && newn.pre!=null && newn.suc!=null)
                    {
                        next=newn;
                        nex=1;
                    }
                    else if (cur==1 && nex==1 && pre==0 && newn.pre!=null && newn.suc!=null)
                    {
                        if (prev.portnum==null) {
                            prev = newn;
                        }
                        else if (prev.portnum!=null) {
                            pq1.add(prev);
                            Log.e("pq1 add", prev.portnum);
                            prev=newn;
                        }
                    }
                    else
                    {
                        pq1.add(newn);
                        Log.e("pq1 add", newn.portnum);
                    }
                    Log.e("loop end", "end");
                }

                if (nex==1)
                {
                    current.pre=next.pre;
                    current.suc=prev.suc;
                    next.pre=current.portnum;
                    prev.suc=current.portnum;
                    pq1.add(current);
                    pq1.add(next);
                    pq1.add(prev);
                    if (start.portnum!=null) {
                        if (!start.portnum.equals(prev.portnum)) {
                        pq1.add(start); }
                    }
                }
                else if (nex==0)
                {
                    current.pre=start.pre;
                    current.suc=prev.suc;
                    start.pre=current.portnum;
                    prev.suc=current.portnum;
                    pq1.add(current);
                    pq1.add(start);
                    pq1.add(prev);
                }
                pq1.drainTo(pq);
            }
            return current;
        }
    }


    public class ClientTaskNJ  extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(REMOTE_PORT[0]));
                PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                BufferedReader br = new BufferedReader(isr);
                String msgToSend = "N ";
                msgToSend = msgToSend.concat(Myport);
                send.println(msgToSend);
                Log.e("Client new msg ", msgToSend);
                String fromserver = br.readLine();
                String[] ports = fromserver.split("[\\s]");
                predecessor = ports[0];
                successor = ports[1];
                Log.e("clientNJ", predecessor+" "+successor);
                socket.close();
            } catch (Exception e) {
                Log.e("ClientNJ Exception", e.toString());
            }
            return null;
        }
    }

    public class ClientTell  {
        public void clientupdate(String... msgs) {
            try {
                while(pq.size()>0) {
                    NodeObject no=pq.poll();
                    pq1.add(no);
                    if (no.pre!=null && no.suc!=null) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(no.portnum));
                        PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                        String msgToSend = "U ";
                        msgToSend = msgToSend.concat(no.pre);
                        msgToSend = msgToSend.concat(" ");
                        msgToSend = msgToSend.concat(no.suc);
                        Log.e("ClientTell new msg ", msgToSend);
                        send.println(msgToSend);
                        socket.close();
                    }
                }
                pq1.drainTo(pq);
            } catch (Exception e) {
                Log.e("ClientTell Exception", e.toString());
            }
        }
    }


    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.e("socket", String.valueOf(sockets[0]));
            try {
                while (true) {
                    Socket s = serverSocket.accept();
                    Log.e("Server", "accept");
                    InputStreamReader isr = new InputStreamReader(s.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
                    String input = br.readLine();
                    if (input.charAt(0)=='N') {
                        Log.e("Server NJ request", input);
                        String[] temp = input.split(" ");
                        Log.e(temp[0],temp[1]);
                        String newnode = temp[1];
                        NodeObject nodeinfo ;
                        Addnode an = new Addnode();
                        nodeinfo = an.addnew(newnode);
                        String out = nodeinfo.pre;
                        out = out.concat(" ");
                        out = out.concat(nodeinfo.suc);
                        pw.println(out);
                        Log.e("node join server", out);

                        ClientTell c=new ClientTell();
                        c.clientupdate();
                    }
                    else if (input.charAt(0)=='U') {
                        Log.e("Server Up request", input);
                        String[] temp = input.split(" ");
                        Log.e(Myport,temp[1]+" "+temp[2]);
                        predecessor = temp[1];
                        successor  = temp[2];
                    }
                    else if(input.charAt(0)=='I') {
                        String[] temp = input.split(" ");
                        String key=temp[1];
                        String value=temp[2];
                        ContentValues mNewValues = new ContentValues();
                        mNewValues.put("key", key);
                        mNewValues.put("value", value);
                        insert(mUri, mNewValues);
                    }
                    else if(input.charAt(0)=='Q' && input.charAt(1)=='S') {
                        String[] temp = input.split(" ");
                        String key=temp[1];
                        Cursor resultCursor =query(mUri, null, key, null, null);
                        Log.e("QS CC", String.valueOf(resultCursor.getCount()));
                        resultCursor.moveToFirst();
                        String returnKey = resultCursor.getString(0);
                        String returnValue = resultCursor.getString(1);
                        String msg=returnKey;
                        msg=msg.concat(" ");
                        msg=msg.concat(returnValue);
                        pw.println(msg);
                        resultCursor.close();
                    } else if(input.charAt(0)=='Q' && input.charAt(1)=='A') {
                        String[] temp = input.split(" ");
                        String port=temp[1];
                        String selection="#-"+port;
                        if (successor.equals(port))
                        {
                            selection="@";
                            Log.e("Suc=port in QA", successor+ " "+ port);
                        }
                        Cursor resultCursor =query(mUri, null, selection, null, null);
                        Log.e("QA CC", String.valueOf(resultCursor.getCount()));
                        int c=0;
                        String msg="-";
                        resultCursor.moveToFirst();
                        while (!resultCursor.isAfterLast()) {
                                if (c>0)
                                {
                                    msg=msg.concat("-");
                                }
                                c++;
                                String returnKey = resultCursor.getString(0);
                                String returnValue = resultCursor.getString(1);
                                msg=msg.concat(returnKey);
                                msg=msg.concat(" ");
                                msg=msg.concat(returnValue);
                                resultCursor.moveToNext();
                            }
                        pw.println(msg);
                    resultCursor.close();
                    } else if(input.charAt(0)=='D' && input.charAt(1)=='S') {
                        String[] temp = input.split(" ");
                        String selection=temp[1];
                        delete(mUri, selection, null);
                    } else if(input.charAt(0)=='D' && input.charAt(1)=='A') {
                        String[] temp = input.split(" ");
                        String port = temp[1];
                        String selection = "#-" + port;
                        if (successor.equals(port)) {
                            selection = "@";
                            Log.e("Suc=port in DA", successor + " " + port);
                        }
                        delete(mUri, selection, null);
                    }
                    br.close();
                    s.close();
                    }

            }catch (Exception e) {
                Log.e("Server Exception", e.toString());
            }
            return null;
        }
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String FILENAME=selection;
        Context context=getContext();

        try {
            String filepath = context.getFilesDir().getAbsolutePath();
            if (predecessor == "0" && successor == "0") {
                if (selection.equals("*") || selection.equals("@")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        file[i].delete();
                        Log.e("File deleted", fname);
                    }
                } else {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        if (fname.equals(selection)) {
                            file[i].delete();
                            Log.e("File deleted", selection);
                            break;
                        }
                    }
                }
            } else {
                String hashkey = genHash(FILENAME);
                String hashpre = genHash(String.valueOf(Integer.parseInt(predecessor) / 2));
                String hashcur = genHash(String.valueOf(Integer.parseInt(Myport) / 2));
                if (selection.equals("@")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        file[i].delete();
                        Log.e("File deleted", fname);
                    }
                } else if (selection.equals("*")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        file[i].delete();
                        Log.e("File deleted", fname);
                    }
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successor));
                        PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                        String msgToSend="DA";
                        msgToSend=msgToSend.concat(" ");
                        msgToSend=msgToSend.concat(Myport);
                        send.println(msgToSend);
                        socket.close();
                } else if (selection.charAt(0)=='#') {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        file[i].delete();
                        Log.e("File deleted", fname);
                    }
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                    String port=selection.split("-")[1];
                    String msgToSend="DA";
                    msgToSend=msgToSend.concat(" ");
                    msgToSend=msgToSend.concat(port);
                    send.println(msgToSend);
                    socket.close();
                } else {
                    if ((hashcur.compareTo(hashpre) < 0) && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) {
                        File f = new File(filepath);
                        File file[] = f.listFiles();
                        for (int i = 0; i < file.length; i++) {
                            String fname = file[i].getName();
                            if (fname.equals(selection)) {
                                file[i].delete();
                                Log.e("File deleted", selection);
                                break;
                            }
                        }
                    } else if (hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0) {
                        File f = new File(filepath);
                        File file[] = f.listFiles();
                        for (int i = 0; i < file.length; i++) {
                            String fname = file[i].getName();
                            if (fname.equals(selection)) {
                                file[i].delete();
                                Log.e("File deleted", selection);
                                break;
                            }
                        }
                    } else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successor));
                        PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                        String msgToSend = "DS ";
                        msgToSend = msgToSend.concat(selection);
                        send.println(msgToSend);
                        socket.close();
                    }
                }
            }
        }catch (Exception e) {
                e.printStackTrace();
            }
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String FILENAME = (String)values.get("key");
        String data = (String) values.get("value");
        Context context=getContext();
        FileOutputStream fos = null;
        try {
            if (predecessor=="0" && successor=="0")
            {
                fos = context.openFileOutput(FILENAME, context.MODE_PRIVATE);
                fos.write(data.getBytes());
                fos.close();
                Log.v("insert 0", values.toString());
            }
            else {
                String hashkey = genHash(FILENAME);
                String hashpre = genHash(String.valueOf(Integer.parseInt(predecessor)/2));
                String hashcur = genHash(String.valueOf(Integer.parseInt(Myport)/2));
                if ((hashcur.compareTo(hashpre) < 0) && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) {
                    fos = context.openFileOutput(FILENAME, context.MODE_PRIVATE);
                    fos.write(data.getBytes());
                    fos.close();
                    Log.v("insert 1", values.toString());
                } else if (hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0) {
                    fos = context.openFileOutput(FILENAME, context.MODE_PRIVATE);
                    fos.write(data.getBytes());
                    fos.close();
                    Log.v("insert 2", values.toString());
                } else {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                    String msgToSend = "I ";
                    msgToSend = msgToSend.concat(FILENAME);
                    msgToSend = msgToSend.concat(" ");
                    msgToSend = msgToSend.concat(data);
                    send.println(msgToSend);
                    Log.v("insert 3", values.toString());
                    socket.close();
                }

            }
        }catch (Exception e) {
            Log.e("Insert Exception", e.toString());
        }
        return uri;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        FileInputStream fis=null;
        String FILENAME=selection;
        String data=null;
        Context context=getContext();
        String filepath = context.getFilesDir().getAbsolutePath();
        MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value"});
        try {
            if (predecessor=="0" && successor=="0") {
                if (selection.equals("*") || selection.equals("@")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        fis = context.openFileInput(fname);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", file[i].getName());
                        builder.add("value", data);
                        Log.v("query 1", selection);
                    }
                } else {
                    fis = context.openFileInput(FILENAME);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                    data = reader.readLine();
                    fis.close();
                    reader.close();
                    Log.v("data", data);
                    MatrixCursor.RowBuilder builder = cursor.newRow();
                    builder.add("key", selection);
                    builder.add("value", data);
                    Log.v("query 2", selection);
                }
            }
            else
            {
                String hashkey = genHash(FILENAME);
                String hashpre = genHash(String.valueOf(Integer.parseInt(predecessor)/2));
                String hashcur = genHash(String.valueOf(Integer.parseInt(Myport)/2));

                if (selection.equals("@")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        fis = context.openFileInput(fname);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", file[i].getName());
                        builder.add("value", data);
                    }
                    Log.v("query @", selection);
                } else if (selection.equals("*")) {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    String msgToSend = "QA ";
                    msgToSend = msgToSend.concat(Myport);
                    send.println(msgToSend);
                    String fromserver=br.readLine();
                    String[] temp1=fromserver.split("-");
                    for (int k=1; k<temp1.length; k++)
                    {
                        String[] temp2=temp1[k].split(" ");
                        String key=temp2[0];
                        String value=temp2[1];
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", key);
                        builder.add("value", value);
                    }
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        fis = context.openFileInput(fname);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        String key=file[i].getName();;
                        String value=data;
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", key);
                        builder.add("value", value);
                    }
                    socket.close();
                    Log.v("query *", selection);
                } else if (selection.charAt(0)=='#') {
                    File f = new File(filepath);
                    File file[] = f.listFiles();
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor));
                    PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                    InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                    BufferedReader br = new BufferedReader(isr);
                    String port=selection.split("-")[1];
                    String msgToSend = "QA ";
                    msgToSend = msgToSend.concat(port);
                    send.println(msgToSend);
                    String fromserver=br.readLine();
                    String[] temp1=fromserver.split("-");
                    for (int k=1; k<temp1.length; k++) {
                        String[] temp2=temp1[k].split(" ");
                        String key=temp2[0];
                        String value=temp2[1];
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", key);
                        builder.add("value", value);
                    }
                    for (int i = 0; i < file.length; i++) {
                        String fname = file[i].getName();
                        fis = context.openFileInput(fname);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        String key=file[i].getName();;
                        String value=data;
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", key);
                        builder.add("value", value);
                    }
                    socket.close();
                    Log.v("query #", selection);
                } else {
                    if ((hashcur.compareTo(hashpre) < 0) && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) {
                        fis = context.openFileInput(FILENAME);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        Log.v("data", data);
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", selection);
                        builder.add("value", data);
                        Log.v("query 3", selection);
                    } else if (hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0) {
                        fis = context.openFileInput(FILENAME);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
                        data = reader.readLine();
                        fis.close();
                        reader.close();
                        Log.v("data", data);
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", selection);
                        builder.add("value", data);
                        Log.v("query 4", selection);
                    } else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successor));
                        PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
                        InputStreamReader isr = new InputStreamReader(socket.getInputStream());
                        BufferedReader br = new BufferedReader(isr);
                        String msgToSend = "QS ";
                        msgToSend = msgToSend.concat(selection);
                        send.println(msgToSend);
                        String fromserver=br.readLine();
                        String key=fromserver.split(" ")[0];
                        String value=fromserver.split(" ")[1];
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add("key", key);
                        builder.add("value", value);
                        Log.v("query 5", selection);
                        socket.close();
                    }
                }
            }
        } catch (Exception e) {
            Log.v("Query Exception", e.toString());
        }
        return cursor;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
}

