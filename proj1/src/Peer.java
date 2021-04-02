import java.io.*;
import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;


public class Peer implements RMI{
    HashMap<String, Set<String>> replicationDegreeMap = new HashMap<>();
    HashMap<String, Integer> desiredRepDegree = new HashMap<>();
    HashMap<String, Boolean> restore = new HashMap<>();
    ScheduledExecutorService threadPool;
    ScheduledExecutorService backupProtocolThreadPool;

    String protocolVersion;
    String peerID;
    String accessPoint;
    
    Listener controlListener;
    Listener dataListener;
    Listener recoveryListener;

    public Peer(String[] args) throws Exception {
        this.protocolVersion = args[0];
        this.peerID = args[1];
        this.accessPoint = args[2];

        String multicastControl = args[3];
        String multicastData = args[4];
        String multicastRecovery = args[5];

        this.threadPool = Executors.newScheduledThreadPool(15);
        this.backupProtocolThreadPool = Executors.newScheduledThreadPool(10);

        this.controlListener = new Listener(multicastControl, this);

        this.dataListener = new Listener(multicastData, this);

        this.recoveryListener = new Listener(multicastRecovery, this);
    }

    public static void main(String[] args) throws Exception{
        Peer peer = new Peer(args);
        peer.joinMulticast();
        peer.setupRMI();

//        if(args[1].equals("1"))
//        {
//            peer.backup("../peer1/test1.txt",1);
//        }
//         create instance of directory
//        File dir = new File("../peer2/b5c31849b58e55aa2444b4114b94d995d16a54b0c60de7dade28f8e4ff10a980");
//
//        // create obejct of PrintWriter for output file
//        PrintWriter pw = new PrintWriter("output.txt");
//
//        // Get list of all the files in form of String Array
//        String[] fileNames = dir.list();
//        Arrays.sort(fileNames, Comparator.comparingInt((String a) -> Integer.parseInt(a.split("_")[1].split("\\.")[0])));
//        // loop for reading the contents of all the files
//        // in the directory GeeksForGeeks
//
//        for (String fileName : fileNames) {
//            Path fileP = Path.of("../peer2/b5c31849b58e55aa2444b4114b94d995d16a54b0c60de7dade28f8e4ff10a980/"+fileName);
//            pw.print(new String(Files.readAllBytes(fileP)));
//            pw.flush();
//        }
//        System.out.println("Reading from all files" +
//                " in directory " + dir.getName() + " Completed");
    }

    private void setupRMI() throws RemoteException, AlreadyBoundException {
        RMI stub = (RMI) UnicastRemoteObject.exportObject(this,0);
        // Bind the remote object's stub in the registry
        Registry registry = LocateRegistry.getRegistry();
        registry.bind(this.accessPoint, stub);
        System.err.println("Server ready");
    }


    public void backup(String filePath, Integer ReplicationDegree) throws Exception {

        byte[] pack = new byte[64000];
        Integer bytesRead = 0, currentChunk = 0, lastBytesRead=0;
        FileInputStream fileInput = new FileInputStream(new File(filePath));
        String fileID = this.makeFileID(filePath);
        while ((bytesRead = fileInput.read(pack)) != -1)
        {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            Integer finalCurrentChunk = currentChunk;
            this.backupProtocolThreadPool.execute(()->
            {
                try {
                    this.sendPutchunk(fileID, finalCurrentChunk.toString(), ReplicationDegree.toString(), body);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            currentChunk++;
            lastBytesRead = bytesRead;
        }
        if(lastBytesRead == 64000) {
            Integer finalCurrentChunk1 = currentChunk;
            this.backupProtocolThreadPool.execute(()->
            {
                try {
                    this.sendPutchunk(fileID, finalCurrentChunk1.toString(), ReplicationDegree.toString(), "".getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void sendPutchunk(String fileID, String chunkNO, String replicationDegree, byte[] body) throws Exception {

        do {
            this.sendPacket("PUTCHUNK", fileID, chunkNO, replicationDegree, body);
            Thread.sleep(1000);
        } while (this.replicationDegreeMap.get(fileID + "_" + chunkNO) == null ||
                (this.replicationDegreeMap.get(fileID + "_" + chunkNO).size() < this.desiredRepDegree.get(fileID + "_" + chunkNO)));
    }

    public void printMsg(String messageType, String peerID)
    {
        System.out.println("[Peer " + this.peerID + "] Received message of type " + messageType + " from peer " + peerID);
    }

    public void interpretMessage(Message msg) throws Exception {
        //Ignore own messages
        if(msg.peerID.equals(this.peerID))
            return;

        String fileChunk = msg.fileID + "_" + msg.chunkNO;
        printMsg(msg.messageType, msg.peerID);
        switch(msg.messageType){
            case "PUTCHUNK":
                this.threadPool.schedule(() -> {
                    try {
                        this.putchunk(msg);
                        this.desiredRepDegree.put(fileChunk, Integer.parseInt(msg.replicationDegree));

                        this.replicationDegreeMap.put(fileChunk, new HashSet<>());
                        this.replicationDegreeMap.get(fileChunk).add(this.peerID);

                        this.restore.put(fileChunk, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, new Random().nextInt(400), TimeUnit.MILLISECONDS);
                break;
            case "GETCHUNK":
                this.threadPool.schedule(() -> {

                    try {
                        this.getchunk(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, new Random().nextInt(400), TimeUnit.MILLISECONDS);

                break;
            case "DELETE":
                this.threadPool.execute(() -> {
                    try {
                        this.deletechunk(msg.fileID);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                break;
            case "REMOVED":

                this.threadPool.schedule(() -> {
                    try {
                        this.updateRepDegreeRemove(msg);

                        if(this.replicationDegreeMap.get(fileChunk)!= null && this.replicationDegreeMap.get(fileChunk).contains(this.peerID))
                            if (this.replicationDegreeMap.get(fileChunk).size() < this.desiredRepDegree.get(fileChunk)) {
                                this.restore.put(fileChunk, true);
                                if (this.restore.get(fileChunk)) {
                                    String filename = "../peer" + this.peerID + "/" + msg.fileID + "/" + fileChunk + ".txt";
                                    Path filePath = Path.of(filename);
                                    byte[] body = Files.readAllBytes(filePath);

                                    this.sendPutchunk(msg.fileID, msg.chunkNO, this.desiredRepDegree.get(fileChunk).toString(), body);
                                }
                            }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }, new Random().nextInt(400), TimeUnit.MILLISECONDS);

                break;
            case "STORED":
                this.updateRepDegreeAdd(msg);
                break;
            case "CHUNK":
                this.saveChunk(msg.fileID, msg.chunkNO, msg.body);
                break;

        }
    }

    private void updateRepDegreeAdd(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;
        if(this.replicationDegreeMap.get(file) != null)
            if(!this.replicationDegreeMap.get(file).contains(msg.peerID))
                this.replicationDegreeMap.get(msg.fileID + "_" + msg.chunkNO).add(msg.peerID);
    }

    private void updateRepDegreeRemove(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;
        if(this.replicationDegreeMap.get(file) != null)
            this.replicationDegreeMap.get(file).remove(msg.peerID);
    }

    private void deletechunk(String fileID) {

        File file = new File("../peer" + this.peerID + "/" + fileID);

        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                f.delete();
            }
        }
        file.delete();
    }

    private void getchunk(Message msg) {
        String filename = "../peer" + this.peerID + "/" + msg.fileID + "/" + msg.fileID + "_" + msg.chunkNO + ".txt";
        try{
            Path filePath = Path.of(filename);
            byte[] body = Files.readAllBytes(filePath);
            this.sendPacket("CHUNK",msg.fileID,msg.chunkNO,null,body);
        }
        catch (Exception e){
            System.out.println("Chunk does not exist on this peer's file system");
        }
    }

    private void putchunk(Message msg) throws Exception {
        saveChunk(msg.fileID, msg.chunkNO, msg.body);

        this.sendPacket("STORED",msg.fileID,msg.chunkNO,null, "".getBytes());
    }

    private void saveChunk(String fileID, String chunkNO, byte[] body) throws IOException {
        //fileID_chunkNO.txt
        File dir = new File("../peer" + this.peerID + "/" + fileID);

        if (!dir.exists()){
            dir.mkdirs();
        }

        String filename = dir+ "/" + fileID + "_" + chunkNO;
        File file = new File(filename);
        file.delete();
        if(file.createNewFile()) {
            Files.write(file.toPath(), body);
        }
    }

    public void joinMulticast() throws Exception {
        this.controlListener.startThread();
        this.dataListener.startThread();
        this.recoveryListener.startThread();
    }

    private byte[] makeHeader(String msgType, String fID, String chunkNO, String repDegree){
        String version = this.protocolVersion;
        String senderID = this.peerID;
        String chunkNumber = "";
        if(chunkNO != null)
            chunkNumber = " " + chunkNO;
        String replicationDegree = "";
        if(repDegree != null)
            replicationDegree = " " + repDegree;

        String finish = version + " " + msgType + " " + senderID + " " + fID + chunkNumber + replicationDegree + " " + '\r' + '\n' + '\r' + '\n';
        return finish.getBytes(StandardCharsets.UTF_8);
    }

    private String makeFileID(String filePath) throws NoSuchAlgorithmException {
        StringBuilder ret = new StringBuilder();
        File f = new File(filePath);
        String absPath = f.getAbsolutePath();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(absPath.getBytes(StandardCharsets.UTF_8));
        for(byte b : hash){
            ret.append(String.format("%02x",b));
        }
        return ret.toString();
    }

    private void sendPacket(String messageType, String fileID,String chunkNO, String replicationDegree,byte[] body) throws IOException {
        byte[] header = this.makeHeader(messageType,fileID,chunkNO, replicationDegree);

        int aLen = header.length;
        int bLen = body.length;
        byte[] result = new byte[aLen + bLen];

        arraycopy(header, 0, result, 0, aLen);
        arraycopy(body, 0, result, aLen, bLen);

        InetAddress groupToSend = null;
        Integer portToSend = null;
        MulticastSocket socketToSend = null;
        switch(messageType){
            case "PUTCHUNK":
                groupToSend = this.dataListener.group;
                portToSend = this.dataListener.port;
                socketToSend = this.dataListener.socket;
                Set<String> set = new HashSet<>();
                this.replicationDegreeMap.put(fileID + "_" + chunkNO, set);
                if(this.replicationDegreeMap.get(fileID + "_" + chunkNO) == null)
                    System.out.println("\n\n\nboda\n\n");
                this.desiredRepDegree.put(fileID + "_" + chunkNO, Integer.parseInt(replicationDegree));
                break;
            case "GETCHUNK":
            case "DELETE":
            case "REMOVED":
            case "STORED":
                groupToSend = this.controlListener.group;
                portToSend = this.controlListener.port;
                socketToSend = this.controlListener.socket;
                break;
            case "CHUNK":
                groupToSend = this.recoveryListener.group;
                portToSend = this.recoveryListener.port;
                socketToSend = this.recoveryListener.socket;
                break;
        }

        DatagramPacket pack = new DatagramPacket(result, result.length, groupToSend, portToSend);
        socketToSend.send(pack);
    }
}
