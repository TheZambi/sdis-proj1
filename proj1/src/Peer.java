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
import java.util.*;


public class Peer {
    HashMap<String, List<String>> replicationDegreeMap = new HashMap<String, List<String>>();

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

        if(args[1].equals("1")) {
            String filePath = "../peer" + peer.peerID + "/" + "test1.txt";
            String fileID = peer.makeFileID(filePath);

            Path fileName = Path.of(filePath);
            String body = Files.readString(fileName);

            do {
                System.out.println("Sending msg");
                peer.sendPacket("PUTCHUNK", fileID, "1", "1", body);
                Thread.sleep(1000);
                System.out.println(peer.replicationDegreeMap.get(fileID + "_" + "1").toString());
            }while(peer.replicationDegreeMap.get(fileID + "_" + "1").size() < 1);
            peer.sendPacket("GETCHUNK", fileID, "1", "", "");

        }
    }

    public void interpretMessage(List<String> msg) throws NoSuchAlgorithmException, IOException {
        //Ignore own messages
        if(msg.get(2).equals(this.peerID))
            return;

        switch(msg.get(1)){
            case "PUTCHUNK":
                Thread savingChunk = new Thread(() -> {
                    System.out.println("PUTCHUNK");
                    System.out.println(msg.get(3));

                    try {
                        this.putchunk(msg);
                        this.replicationDegreeMap.put( msg.get(3) + "_" + msg.get(4), new ArrayList<>());
                        this.replicationDegreeMap.get(msg.get(3) + "_" + msg.get(4)).add(this.peerID);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                savingChunk.start();
                break;
            case "GETCHUNK":
                Thread getChunk = new Thread(() -> {
                    System.out.println("GETCHUNK");

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
                    System.out.println("DELETE");

                    try {
                        this.deletechunk(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                deleteChunk.start();
                break;
            case "REMOVED":
                System.out.println("REMOVED");
                break;
            case "STORED":
                System.out.println("STORED");
                this.updateRepDegree(msg);
                break;
            case "CHUNK":
                System.out.println("CHUNK");
                System.out.println(msg.get(5));
                break;

        }
    }

    private void updateRepDegree(List<String> msg) {
        if(this.replicationDegreeMap.get(msg.get(3) + "_" + msg.get(4)) != null)
            if(!this.replicationDegreeMap.get(msg.get(3) + "_" + msg.get(4)).contains(msg.get(2)))
                this.replicationDegreeMap.get(msg.get(3) + "_" + msg.get(4)).add(msg.get(2));
    }

    private void deletechunk(List<String> msg) {

        File file = new File("../peer" + this.peerID + "/" + msg.get(3));

        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                f.delete();
            }
        }
        file.delete();
    }

    private void getchunk(List<String> msg) throws IOException, NoSuchAlgorithmException {
        String filename = "../peer" + this.peerID + "/" + msg.get(3) + "/" + msg.get(3)+"_"+msg.get(4)+".txt";
        try{
            Path filePath = Path.of(filename);
            String body = Files.readString(filePath);
            Random rand = new Random();
            Thread.sleep(rand.nextInt(400));
            this.sendPacket("CHUNK",msg.get(3),msg.get(4),null,body);
        }
        catch (Exception e){
            System.out.println("Chunk does not exist on this peer's file system");
        }
    }

    private void putchunk(List<String> msg) throws Exception {
        //fileID_chunkNO.txt

        File dir = new File("../peer" + this.peerID + "/" + msg.get(3));
        if (!dir.exists()){
            dir.mkdirs();
        }

        String filename = dir+ "/" + msg.get(3)+"_"+msg.get(4)+".txt";
        File file = new File(filename);
        if (file.createNewFile()) {
            FileWriter writer = new FileWriter(filename);
            writer.write(msg.get(6));
            writer.close();
        }

        Random rand = new Random();
        Thread.sleep(rand.nextInt(400));
        this.sendPacket("STORED",msg.get(3),msg.get(4),null,"");

    }

    public void joinMulticast() throws Exception {
        this.controlListener.startThread();
        this.dataListener.startThread();
        this.recoveryListener.startThread();
    }

    private byte[] makeHeader(String msgType, String fID, String chunkNO, String repDegree) throws NoSuchAlgorithmException {
        String version = this.protocolVersion;
        String messageType = msgType;
        String senderID = this.peerID;
        String fileID = fID;
        String chunkNumber = "";
        if(chunkNO != null)
            chunkNumber = " " + chunkNO;
        String replicationDegree = "";
        if(repDegree != null)
            replicationDegree = " " + repDegree;

        String finish = version + " " + messageType + " " + senderID + " " + fileID + chunkNumber + replicationDegree + " " + '\r' + '\n' + '\r' + '\n';
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


    public List<String> parseMessage(DatagramPacket recv) {
        List<String> parsedMessage = new ArrayList<>();
        String s = new String(recv.getData()).substring(0,recv.getLength()); //ignores following \0
        String[] arr = s.split("\r\n\r\n"); //separates body from header
        String header = arr[0];
        String body = "";
        if(arr.length ==2)
            body = arr[1];
        String[] aux = header.split(" ",4);
        parsedMessage.add(aux[0]); //version of protocol
        parsedMessage.add(aux[1]); // command to be made
        parsedMessage.add(aux[2]); //peer id
        String fileID = "";
        for(int i=0; i<64; i++)
            fileID += aux[3].charAt(i); //gets the next 64 bytes which correspond to the encrypted file id

        parsedMessage.add(fileID);
        String[] left = aux[3].substring(65).split(" "); //gets the arguments that might come after
        if(!left[0].equals(""))
            parsedMessage.addAll(Arrays.asList(left)); //if there are any arguments in the array left they are added to the parsedMessage

        parsedMessage.add(body); //adds the body to the parsedMessage
        return parsedMessage;
    }

    private void sendPacket(String messageType, String fileID,String chunkNO, String replicationDegree,String body) throws NoSuchAlgorithmException, IOException {
        byte[] header = this.makeHeader(messageType,fileID,chunkNO, replicationDegree);
        byte[] msgBody = body.getBytes();

        int aLen = header.length;
        int bLen = msgBody.length;
        byte[] result = new byte[aLen + bLen];

        System.arraycopy(header, 0, result, 0, aLen);
        System.arraycopy(msgBody, 0, result, aLen, bLen);

        InetAddress groupToSend = null;
        Integer portToSend = null;
        MulticastSocket socketToSend = null;
        switch(messageType){
            case "PUTCHUNK":
                groupToSend = this.dataListener.group;
                portToSend = this.dataListener.port;
                socketToSend = this.dataListener.socket;
                this.replicationDegreeMap.put(fileID + "_" + chunkNO, new ArrayList<>());
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
