import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static java.lang.System.arraycopy;


public class Peer {
    HashMap<String, List<String>> replicationDegreeMap = new HashMap<>();
    HashMap<String, Integer> desiredRepDegree = new HashMap<>();
    HashMap<String, Boolean> restore = new HashMap<>();

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

        this.controlListener = new Listener(multicastControl, this);

        this.dataListener = new Listener(multicastData, this);

        this.recoveryListener = new Listener(multicastRecovery, this);
    }

    public static void main(String[] args) throws Exception{
        Peer peer = new Peer(args);
        peer.joinMulticast();

        String filePath = "../peer1/" + "test1.txt";
        String fileID = peer.makeFileID(filePath);
        if(args[1].equals("1")) {

            Path fileName = Path.of(filePath);
            String body = Files.readString(fileName);

            peer.sendPutchunk(fileID, "1", "2", body);
        }
    }

    private void sendPutchunk(String fileID,String chunkNO,String replicationDegree ,String body) throws Exception {

        do {
            this.sendPacket("PUTCHUNK", fileID, chunkNO, replicationDegree, body);
            Thread.sleep(1000);
        }while(this.replicationDegreeMap.get(fileID + "_" + "1").size() < this.desiredRepDegree.get(fileID + "_" + "1"));

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
                if(!this.peerID.equals("1")) {
                    Thread savingChunk = new Thread(() -> {
                        try {
                            this.putchunk(msg);
                            this.desiredRepDegree.put(fileChunk, Integer.parseInt(msg.replicationDegree));
                            if (this.replicationDegreeMap.get(fileChunk) == null) {
                                this.replicationDegreeMap.put(fileChunk, new ArrayList<>());
                                this.replicationDegreeMap.get(fileChunk).add(this.peerID);
                            }
                            this.restore.put(fileChunk, false);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    savingChunk.start();
                }
                break;
            case "GETCHUNK":
                Thread getChunk = new Thread(() -> {

                    try {
                        this.getchunk(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                getChunk.start();

                break;
            case "DELETE":

                Thread deleteChunk = new Thread(() -> {

                    try {
                        this.deletechunk(msg.fileID);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                deleteChunk.start();
                break;
            case "REMOVED":

                Thread removedChunk = new Thread(() -> {
                    try {
                        this.updateRepDegreeRemove(msg);

                        if(this.replicationDegreeMap.get(fileChunk)!= null && this.replicationDegreeMap.get(fileChunk).contains(this.peerID))
                            if (this.replicationDegreeMap.get(fileChunk).size() < this.desiredRepDegree.get(fileChunk)) {
                                this.restore.put(fileChunk, true);
                                Random rand = new Random();
                                Thread.sleep(rand.nextInt(400));
                                if (this.restore.get(fileChunk)) {
                                    String filename = "../peer" + this.peerID + "/" + msg.fileID + "/" + fileChunk + ".txt";
                                    Path filePath = Path.of(filename);
                                    String body = Files.readString(filePath);

                                    this.sendPutchunk(msg.fileID, msg.chunkNO, this.desiredRepDegree.get(fileChunk).toString(), body);
                                }
                            }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                });

                removedChunk.start();
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
            String body = Files.readString(filePath);
            Random rand = new Random();
            Thread.sleep(rand.nextInt(400));
            this.sendPacket("CHUNK",msg.fileID,msg.chunkNO,null,body);
        }
        catch (Exception e){
            System.out.println("Chunk does not exist on this peer's file system");
        }
    }

    private void putchunk(Message msg) throws Exception {


        saveChunk(msg.fileID, msg.chunkNO, msg.body);
        Random rand = new Random();
        Thread.sleep(rand.nextInt(400));
        this.sendPacket("STORED",msg.fileID,msg.chunkNO,null,"");

    }

    private void saveChunk(String fileID, String chunkNO, String body) throws IOException {
        //fileID_chunkNO.txt
        File dir = new File("../peer" + this.peerID + "/" + fileID);

        if (!dir.exists()){
            dir.mkdirs();
        }

        String filename = dir+ "/" + fileID + "_" + chunkNO + ".txt";
        File file = new File(filename);
        file.delete();
        if(file.createNewFile()) {
            FileWriter writer = new FileWriter(filename);
            writer.write(body);
            writer.close();
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




    private void sendPacket(String messageType, String fileID,String chunkNO, String replicationDegree,String body) throws IOException {
        byte[] header = this.makeHeader(messageType,fileID,chunkNO, replicationDegree);
        byte[] msgBody = body.getBytes();

        int aLen = header.length;
        int bLen = msgBody.length;
        byte[] result = new byte[aLen + bLen];

        arraycopy(header, 0, result, 0, aLen);
        arraycopy(msgBody, 0, result, aLen, bLen);

        InetAddress groupToSend = null;
        Integer portToSend = null;
        MulticastSocket socketToSend = null;
        switch(messageType){
            case "PUTCHUNK":
                groupToSend = this.dataListener.group;
                portToSend = this.dataListener.port;
                socketToSend = this.dataListener.socket;
                if(this.replicationDegreeMap.get(fileID + "_" + chunkNO) == null) {
                    this.replicationDegreeMap.put(fileID + "_" + chunkNO, new ArrayList<>());
                    this.desiredRepDegree.put(fileID + "_" + chunkNO, Integer.parseInt(replicationDegree));
                }
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
