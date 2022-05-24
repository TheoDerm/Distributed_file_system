import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;

public class DataStore {

    private Hashtable<String,Long> listOfFiles = new Hashtable<>();  //a list of files and their sizes
    private int port;
    private BufferedReader in;
    private PrintWriter out;
    private Socket socket;

    public DataStore(ArrayList<String> file_list, int port, BufferedReader in, PrintWriter out,Socket socket) throws IOException {
        this.port = port;
        this.in = in;
        this.socket = socket;
        this.out = out;
        for(String file: file_list) {
            listOfFiles.put(file, (long) 0);  //This is executing once the dstores connect to the client that's why filesize is 0
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public void getPrintWriter(String str) throws IOException {

        PrintWriter out = new PrintWriter(socket.getOutputStream());
        out.println(str);
        out.flush();
    }

    public BufferedReader getBufferedReader(){ return in; }

    public void setBufferedReader(BufferedReader in) {
        this.in = in;
    }

    public int getPort() {
        return port;
    }

    //NEW METHOD
    public void  rebalance (float k) {
       if(getListOfFiles().size() < floor(k)) {
           int num = (int) (floor(k) - getListOfFiles().size());
           System.out.println("R " + num);
       } else if (getListOfFiles().size() > ceil(k)) {

       }
    }



    public Hashtable<String, Long> getListOfFiles() {
        return listOfFiles;
    }

    public long getFileSize(String filename) {
        return listOfFiles.get(filename);
    }

    public String getFile_List() {
        String msg = "";
        for(String str:listOfFiles.keySet()) {
       // for(MyFile file:myFiles) {
            msg = msg + " " + str;
            //msg = msg + " " + file.getName();
        }
        return msg;
    }

    public void removeFile(String filename) {
        listOfFiles.remove(filename);
        //myFiles.remove(filename);
    }

    public boolean fileExists(String filename) {
        return getFile_List().contains(filename);
    }

    public void addFile(String filename, Long size) {
//        if(listOfFiles.containsKey(filename)) {
//
//           // Not sure if this is correct
//
//            System.out.println("File already exists in DataStore");
//            listOfFiles.replace(filename,size);
//        } else {
            listOfFiles.put(filename,size);
//        }

        /*
        boolean flag = true;
        for(MyFile file: myFiles) {
            if(file.getName().equals(filename)) {
                System.out.println("File already exists in datastore");
                flag = false;
                break;
            }
        }
        if(flag) { myFiles.add(new MyFile(filename,size)); }

         */
    }


}
