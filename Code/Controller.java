import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;

public class Controller {

    static final Hashtable <DataStore,Boolean> dataStores = new Hashtable<>();
    //this has the filename and its index status
    static Hashtable<String,String> index = new Hashtable<>();

    static int timeout;


    public static void main(String[] args) throws IOException {
        if(args.length < 4) {
            throw  new IOException("Insufficient amount of arguments");
        }

        int cPort = Integer.parseInt(args[0]);
        int r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        long rebalance_period = Long.parseLong(args[3]);

        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);

        try {
            ServerSocket ss = new ServerSocket(cPort);

            for (; ; ) {
                if (dataStores.size() < r) {
                    System.out.println("Waiting for " + (r-dataStores.size() )+ " DStore to join...");
                } else {
                    System.out.println("Waiting for request...");
                }
                Socket socket = ss.accept();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String line;
                            ArrayList<String> parameters;
                            String command;

                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            for (; ; ) {
                                //check if the connection needs to be closed
                                Thread.sleep(500);
                                line=in.readLine();
                                ControllerLogger.getInstance().messageReceived(socket, line);

                                checkDStoresHeartBeat(dataStores);

                                //    always keep parameters and command together!
                                parameters = findParameters(line);
                                command = findCommand(parameters);

                                if(!command.equals("JOIN") && !command.equals("STORE") && !command.equals("LOAD") && !command.equals("RELOAD") && !command.equals("LIST") && !command.equals("REMOVE")) {
                                    System.out.println("Malformed message received");
                                    continue;
                                }

                                if (command.equals("JOIN")) {
                                    joinOperation(socket, parameters, in, out, dataStores);
                                    break;
                                } else if (!checkDStores(r, dataStores)) {
                                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                                    out.flush();
                                    ControllerLogger.getInstance().messageSent(socket, "ERROR_NOT_ENOUGH_DSTORES");
                                    //closeConnections(socket, out, in);
                                    break;
                                } else if (checkValidOperation(socket, index, parameters, command, out)) {
                                    if (command.equals("STORE")) {
                                        try {
                                            storeOperation(socket, index, parameters, out, dataStores, r);

                                        } catch (Exception e) {
                                            index.remove(parameters.get(0));
                                            System.out.println(e.getMessage());
                                        }

                                    } else if (command.equals("LOAD") || command.equals("RELOAD")) {
                                        if (command.equals("LOAD")) {
                                            pickRDStoresToLoad(dataStores, parameters, r);
                                        }
                                        loadOperation(socket,dataStores, parameters, out);

                                    } else if(line.equals("LIST")) {
                                        listOperation(socket, index, out);
                                    } else if (command.equals("REMOVE")) {
                                        try {
                                            removeOperation(socket, index, parameters, out, dataStores, r);
                                        } catch (Exception e) {
                                            System.out.println(e.getMessage());
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }

                    }
                }).start();
            }
        } catch (Exception e) {
            System.out.println("error " + e);
        }
    }

    /**
     * I am sending Beat messages to DStores every time a request is wanted to check if the are alive. If not I delete
     * those dstores that are failing.
     */
    private synchronized static void checkDStoresHeartBeat(Hashtable<DataStore,Boolean> dataStores) throws IOException, InterruptedException {

        ArrayList<DataStore> toRemove = new ArrayList<>();
        for(DataStore dataStore: dataStores.keySet()) {
//           check if the dstores are responding
            try {
                Socket socket = new Socket(InetAddress.getLocalHost(), dataStore.getPort());
                PrintWriter out2 = new PrintWriter(socket.getOutputStream());
                out2.println("BEAT");
                out2.flush();
                ControllerLogger.getInstance().messageSent(socket,"BEAT");
                //Thread.sleep(200);
                if (dataStore.getBufferedReader().readLine() == null) {
                    toRemove.add(dataStore);
                }
                System.out.println("BEAT_ACK");
            } catch (ConnectException e) {
                e.printStackTrace();
                toRemove.add(dataStore);
            }
        }
        for(DataStore d: toRemove) {
                dataStores.remove(d);
        }
    }

    /**
     * I pick the R dstores that have a file stores so I can load it from them
     */
    private static void pickRDStoresToLoad(Hashtable<DataStore,Boolean> dataStores, ArrayList<String> parameters,int r) {
        //make sure datastores has all false values
        unselectDataStores(dataStores);

        if (parameters.size() < 1) {
            System.out.println("Error in parameters");
        }
        String filename = parameters.get(0);
        //Chooses the R DStores that store the file
        int l = 1;
        for (DataStore k : dataStores.keySet()) {
            if (l > r) break;
            if (k.fileExists(filename)) {
                dataStores.replace(k, true);
                l++;
            }
        }
    }

    /**
     * Checking the validity of each operation
     * */
    private static boolean checkValidOperation(Socket socket,Hashtable<String,String> index, ArrayList<String> parameters, String command,PrintWriter out) {
        String filename  = parameters.get(0);

        if (!index.containsKey(filename) ) {
            if(command.equals("REMOVE") || command.equals("LOAD")) {
                out.println("ERROR_FILE_DOES_NOT_EXIST");
                out.flush();
                ControllerLogger.getInstance().messageSent(socket, "ERROR_FILE_DOES_NOT_EXIST");
                return false;
            }
        }
        if(index.containsKey(filename)) {
            if (command.equals("REMOVE") || command.equals("LOAD")) {
                if (index.get(filename).equals("store in progress")) {
                    out.println("ERROR_STORE_IN_PROGRESS");
                    out.flush();
                    ControllerLogger.getInstance().messageSent(socket,"ERROR_STORE_IN_PROGRESS");
                    return false;
                } else if (index.get(filename).equals("remove in progress") || (index.get(filename).equals("remove completed"))) {
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    out.flush();
                    ControllerLogger.getInstance().messageSent(socket,"ERROR_FILE_DOES_NOT_EXIST");
                    return false;
                }
            } else if (command.equals("STORE")) {
                if (index.get(filename).equals("store complete")) {
                    out.println("ERROR_FILE_ALREADY_EXISTS");
                    out.flush();
                    ControllerLogger.getInstance().messageSent(socket,"ERROR_FILE_ALREADY_EXISTS");
                    return false;
                } else if (index.get(filename).equals("remove in progress")) {
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    out.flush();
                    ControllerLogger.getInstance().messageSent(socket,"ERROR_FILE_DOES_NOT_EXIST");
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * From what I understood in the comments on teams controller shouldn't close connections.
     */
    private static void closeConnections(Socket socket,PrintWriter out,BufferedReader in) throws IOException {
        out.close();
        in.close();
        socket.close();
    }

    /**
     * Checks if enough dstores have joined
     */
    private static boolean checkDStores(int r, Hashtable<DataStore,Boolean> dataStores) {
        if(dataStores.size()<r) {
            return false;
        }
        return true;
    }


    private synchronized static void removeOperation(Socket client,Hashtable<String,String> index,ArrayList<String> parameters, PrintWriter out, Hashtable<DataStore, Boolean> dataStores,int r) throws IOException, InterruptedException {
        if(parameters.size() != 1) {
            System.out.println("Wrong command");
            return;
        }
        int dStoresWithFile = 0;
        int numOfACK = 0;
        String filename = parameters.get(0);

        index.replace(filename,"remove in progress");
        System.out.println("remove in progress");
        for(DataStore dataStore : dataStores.keySet()) {
            if(dataStore.getFile_List().contains(filename)) {
                dStoresWithFile++; //im not sure about that
                Socket dsSocket = new Socket(InetAddress.getLocalHost(),dataStore.getPort());
                PrintWriter dsOut = new PrintWriter(dsSocket.getOutputStream());
                dsOut.println("REMOVE " + filename);
                dsOut.flush();

                ControllerLogger.getInstance().messageSent(dsSocket,"REMOVE " + filename);


                //wait for ACK from dStore
                    //Thread.sleep(100);
                    dsSocket.setSoTimeout(timeout);
                    System.out.println("Waiting for ACK...");
                    String line = dataStore.getBufferedReader().readLine();

                    ControllerLogger.getInstance().messageReceived(dsSocket, line);

                    parameters = findParameters(line);
                    String command = findCommand(parameters);
                    if(command.equals("REMOVE_ACK")) {
                        numOfACK ++;
                        break;
                    } else if(command.equals("ERROR_FILE_DOES_NOT_EXIST")) {
                        System.out.println("ERROR_FILE_DOES_NOT_EXIST " + parameters.get(0));
                    }

            }
        }
        if(numOfACK == dStoresWithFile) {
            updateDataStores(dataStores,filename, (long) 0,"removeFile");
            unselectDataStores(dataStores);
            index.replace(filename,"remove complete");
            //not sure about that
            index.remove(filename);
            out.println("REMOVE_COMPLETE");
            out.flush();

            ControllerLogger.getInstance().messageSent(client,"REMOVE_COMPLETE");

            System.out.println("----REMOVE COMPLETED----\n");
        } else {
            System.out.println("Error with ACKs for Removing a file");
        }
    }


    private synchronized static  void loadOperation(Socket socket,Hashtable<DataStore,Boolean> dataStores, ArrayList<String> parameters,PrintWriter out) {
        if(parameters.size() != 1) {
            System.out.println("Error in number of parameters");
            return;
        }
        String filename = parameters.get(0);
        for (DataStore i : dataStores.keySet()) {
            if (dataStores.get(i) && i.fileExists(filename)) {
                String sizeToSend = String.valueOf(i.getFileSize(filename));
                int portToSend = i.getPort();
                dataStores.replace(i, false);
                out.println("LOAD_FROM " + portToSend + " " + sizeToSend);
                out.flush();

                ControllerLogger.getInstance().messageSent(socket,"LOAD_FROM " + portToSend + " " + sizeToSend);

                System.out.println("----LOAD COMPLETED----\n");
                return;
            }
        }
        out.println("ERROR_LOAD");
        out.flush();
        ControllerLogger.getInstance().messageSent(socket,"ERROR_LOAD");
    }


    private static void listOperation(Socket socket,Hashtable<String,String> index, PrintWriter out) {
        String file_list="";
        for(String str:index.keySet()) {
            if(index.get(str).equals("store complete")) {
                if(file_list.equals("")) {
                    file_list = str;
                } else {
                    file_list = file_list + " " + str;
                }
            }
        }
        out.println("LIST " + file_list);
        out.flush();

        ControllerLogger.getInstance().messageSent(socket,"LIST " + file_list);

        System.out.println("----LIST COMPLETED----\n");
    }

    /**
     * Executes the store operation by selecting R DStores and setting their flag value to true
     */
    private synchronized static void storeOperation(Socket socket,Hashtable<String,String> index,ArrayList<String> parameters, PrintWriter out, Hashtable<DataStore, Boolean> dataStores, int r) throws IOException, InterruptedException {
        if(parameters.size()!=2) {
            System.out.println("Error in command");
            return;
        }
        int numOfACK = 0;
        String filename = parameters.get(0);
        Long filesize = Long.parseLong(parameters.get(1));

        index.put(filename,"store in progress");

        //Send the list of ports that the client will store the file
        String msg = "STORE_TO";
        int i = 1;
        //make sure the dstores are not selected be previous operations
        unselectDataStores(dataStores);

        // Chooses R DStores to store the file
        for (DataStore k : dataStores.keySet()) {
            if (i > r) break;
            dataStores.replace(k, true);
            msg = msg + " " + k.getPort();
            i++;
        }

        out.println(msg);
        out.flush();
        ControllerLogger.getInstance().messageSent(socket, msg);

        for (DataStore dataStore : dataStores.keySet()) {
            if (!dataStores.get(dataStore)) {
                continue;
            }
           // Thread.sleep(100);
            dataStore.getSocket().setSoTimeout(timeout);
            String line = dataStore.getBufferedReader().readLine();
            ControllerLogger.getInstance().messageReceived(socket, line);   //not sure about this
            parameters = findParameters(line);
            String command = findCommand(parameters);

            if (command.equals("STORE_ACK")) {
                numOfACK++;

                if(numOfACK == r) {
                    break;
                }
            }
        }
        if(numOfACK == r) {
            index.replace(filename,"store complete");
            //after index is updated we then add the file to dataStores in order to be added for LIST etc..
            updateDataStores(dataStores, filename, filesize,"addFile");

           // out = new PrintWriter(socket.getOutputStream());
            out.println("STORE_COMPLETE");
            out.flush();

            ControllerLogger.getInstance().messageSent(socket, "STORE_COMPLETE");

            System.out.println("---STORE COMPLETED---\n");
        } else {
            System.out.println("Not enough ACKs received for Storing a file");
            index.remove(filename);
        }
    }


    private static void joinOperation(Socket socket, ArrayList<String> parameters,BufferedReader in, PrintWriter out, Hashtable<DataStore, Boolean> dataStores) throws IOException, InterruptedException {
        int port = Integer.parseInt(parameters.get(0));
        //useLogger
        ControllerLogger.getInstance().dstoreJoined(socket, port);

        //this is for the rebalance operation. I haven't implement it.
        out.println("LIST");
        out.flush();

        //useLogger
        ControllerLogger.getInstance().messageSent(socket,"LIST");
       // Thread.sleep(100);
        // DStore sends its list of files
        String line = in.readLine();
        //useLogger
        ControllerLogger.getInstance().messageReceived(socket, line);
        // Always parameters and command together
        parameters = findParameters(line);
        String command = findCommand(parameters);

        // Fills dataStore with the port and its files
        DataStore dataStore = new DataStore(parameters,port,in,out,socket);
        dataStores.put(dataStore,false);

        System.out.println("----JOIN COMPLETED----\n");

    }


    private static void updateDataStores(Hashtable<DataStore,Boolean> dataStores, String filename,Long filesize,String command) {
        for(DataStore dataStore : dataStores.keySet()) {
            if(dataStores.get(dataStore)) {
                if(command.equals("addFile")) {
                    dataStore.addFile(filename,filesize);
                } else if (command.equals("removeFile")) {
                    dataStore.removeFile(filename);
                }
            }
        }
    }


    private static void unselectDataStores(Hashtable<DataStore,Boolean> dataStores) {
              dataStores.replaceAll((i, v) -> false);
    }


    private static ArrayList<String> findParameters(String line) {
        ArrayList<String> parameters = new ArrayList<>(Arrays.asList(line.split(" ")));
        return parameters;
    }


    private static String findCommand(ArrayList<String> parameters) {
        String command;
        if(parameters.size() == 0) {
            System.out.println("Error in command");
            return ("Error");
        }
        if (parameters.size() != 1) {
            command = parameters.get(0);
            parameters.remove(0);

        } else {
            command = parameters.get(0);
        }
        return command;
    }



}
