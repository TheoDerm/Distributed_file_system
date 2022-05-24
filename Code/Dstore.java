import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;



public class Dstore {
    static int timeout;
    public static void main(String [] args) throws IOException {
        if(args.length < 4) {
            throw  new IOException("Insufficient amount of arguments");
        }


        int port = Integer.parseInt(args[0]);
        int cPort = Integer.parseInt(args[1]);
        timeout =Integer.parseInt(args[2]);
        String file_folder = args[3];
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);


        PrintWriter out;
        BufferedReader in;

        Socket cSocket = new Socket(InetAddress.getLocalHost(),cPort);
        PrintWriter cOut = new PrintWriter(cSocket.getOutputStream());
        BufferedReader cIn = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
        ServerSocket ss = new ServerSocket(port);

        // Makes a folder at user.dir with the name of file_folder
        File folder = new File(System.getProperty("user.dir") + "/" + file_folder);
        System.out.println(folder);
        if(folder.mkdir()) {
            System.out.println(file_folder + " created");
        } else {
            System.out.println("File folder: " + file_folder + " could not be created");
        }

        try{
            joinController(cIn,cOut,cSocket,port,folder);

            //Start receiving messages from clients
            for(;;) {
                System.out.println("Waiting for command...");
                Socket client = ss.accept();

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        //for (;;) {
                            try {
                                PrintWriter out = new PrintWriter(client.getOutputStream());
                                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                                String line = in.readLine();
                                DstoreLogger.getInstance().messageReceived(client,line);

                                ArrayList<String> parameters = findParameters(line);
                                String command = findCommand(parameters);


                                if(!command.equals("STORE") && !command.equals("LOAD_DATA") && !command.equals("REMOVE") && !command.equals("BEAT") ) {
                                    System.out.println("Malformed message received");
                                    return;
                                }

                                switch (command) {
                                    case "STORE" -> storeOperation(parameters, out, file_folder,cOut, cSocket, client);
                                    case "LOAD_DATA" -> loadOperation(parameters, folder, client);
                                    case "REMOVE" -> removeOperation(parameters, folder, cOut, cSocket);
                                    case "BEAT" -> {
                                        cOut.println("BEAT_ACK");
                                        cOut.flush();
                                        DstoreLogger.getInstance().messageSent(cSocket, "BEAT_ACK");
                                        System.out.println("BEAT_ACK");
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                       // }



                    }
                }).start();




                //closeConnections(client,in,out);
            }
        }catch(Exception e){System.out.println("error "+e);}
        System.out.println();
    }

    /**
     * From what I understood in the comments on teams dstore shouldn only close client on load operation.
     */
    private static void closeConnections(Socket client,BufferedReader in, PrintWriter out) throws IOException {
        out.close();
        in.close();
        client.close();
    }

    /**
     * receives the message from the client and then finds the files in the directory to delete. If no files are found it sends error
     */
    private synchronized static void removeOperation(ArrayList<String> parameters,File folder,PrintWriter cOut,Socket cSocket) {
        if(parameters.size()!=1) {
            System.out.println("Error in the arguments after the command received");
            return;
        }
        String file_to_delete = parameters.get(0);
        File[] files = folder.listFiles();
        boolean flag = false;
        if (files != null) {
            for (File file: files) {
                if(file.getName().equals(file_to_delete)) {
                    if(file.delete()) {

                        cOut.println("REMOVE_ACK " + file_to_delete);
                        cOut.flush();
                        DstoreLogger.getInstance().messageSent(cSocket,"REMOVE_ACK " + file_to_delete);
                        flag = true;
                        System.out.println("----REMOVE COMPLETED----\n");
                        break;
                    } else {
                        System.out.println("There was a problem deleting file: " + file);
                    }
                }
            }
        } else {
            System.out.println("The folder is empty");
        }
        if(!flag){
            cOut.println("ERROR_FILE_DOES_NOT_EXIST " + file_to_delete);
            cOut.flush();
            DstoreLogger.getInstance().messageSent(cSocket,"ERROR_FILE_DOES_NOT_EXIST " + file_to_delete);
        }
    }

    /**
     * We find the file to load from the list of files of the directory. If none is found then we send error
     */
    private synchronized static void loadOperation(ArrayList<String> parameters,File folder,Socket client) throws IOException {
        if (parameters.size() != 1) {
            System.out.println("Error,wrong number of arguments after the command");
            return;
        }
        //Start loading the file content
        String filename = parameters.get(0);

        File[] file_list = folder.listFiles();
        File wantedFile = null;
        for (File f : Objects.requireNonNull(file_list)) {
            if (f.getName().equals(filename)) {
                wantedFile = f;
                break;
            }
        }
        if(wantedFile == null) {
            client.close();
            return;
        }
        byte[] buf = new byte[(int) Objects.requireNonNull(wantedFile).length()];
        int buflen;
        FileInputStream inf = null;
        inf = new FileInputStream(wantedFile);
        try {
            OutputStream outputStream = client.getOutputStream();
            while ((buflen = inf.read(buf)) != -1) {
                // System.out.print("*");
                outputStream.write(buf, 0, buflen);
            }
        } catch (Exception e) {
            System.out.println("error" + e);
        }
        System.out.println("----LOAD COMPLETED----\n");
    }


    private synchronized static void storeOperation(ArrayList<String> parameters, PrintWriter out,String file_folder,PrintWriter dStoreOut,Socket dStoreSocket,Socket client) throws IOException {
        if (parameters.size() != 2) {
            System.out.println("Error, false number of arguments after the command");
            return;
        }
        String fileName = parameters.get(0);
        Path filePath = Paths.get(fileName);
        String fileSize = parameters.get(1);

        //Send client ACK
        out.println("ACK");
        out.flush();
        DstoreLogger.getInstance().messageSent(client,"ACK");

        //Start storing the file
        InputStream inStream = client.getInputStream();
        byte[] bufLength;
        bufLength = inStream.readNBytes(Integer.parseInt(fileSize));

        File outputFile = new File(System.getProperty("user.dir") + "/" + file_folder + "/" + filePath.getFileName());
        FileOutputStream outStream = new FileOutputStream(outputFile);
        outStream.write(bufLength);
        inStream.close();
        outStream.close();

        //Send dStore ACK
        dStoreOut.println("STORE_ACK " + fileName);
        dStoreOut.flush();
        DstoreLogger.getInstance().messageSent(dStoreSocket,"STORE_ACK " + fileName);

        System.out.println("----STORE COMPLETED----\n");
    }

    /**
     * Initial operation of the dstore in order to establish connection with the controller
     */
    private static void joinController(BufferedReader cIn,PrintWriter cOut, Socket cSocket,int port,File folder) throws IOException {
        cOut.println("JOIN " + port);
        cOut.flush();
        DstoreLogger.getInstance().messageSent(cSocket,"JOIN " + port);

         //this is for the rebalance (I haven't implement it)
        //Message from the controller
        String line = cIn.readLine();
        DstoreLogger.getInstance().messageReceived(cSocket,line);

        ArrayList<String> parameters = findParameters(line);
        String command = findCommand(parameters);

        //Sends a list with the folder's files
        if (command.equals("LIST")) {
            String[] file_list2 = folder.list();
            String msg = "LIST";
            int i=0;

            while(i< Objects.requireNonNull(file_list2).length) {
                msg = msg + " " + file_list2[i];
                i++;
            }

            cOut.println(msg);
            cOut.flush();
            DstoreLogger.getInstance().messageSent(cSocket,msg);

            System.out.println("----JOIN COMPLETED----\n");
        }
    }


    private static ArrayList<String> findParameters(String line) {
        ArrayList<String> parameters = new ArrayList<>(Arrays.asList(line.split(" ")));
        return parameters;
    }


    private static String findCommand(ArrayList<String> parameters) {
        String command = parameters.get(0);
        parameters.remove(0);
        return command;
    }

}
