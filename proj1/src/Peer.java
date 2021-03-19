import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.sleep;


public class Peer {
    String protocolVersion;
    String peerID;
    String accessPoint;

    String multicastAddress;
    Integer multicastPort;
    InetAddress group;
    MulticastSocket multiSocket;

    public Peer(String[] args) throws IOException, NoSuchAlgorithmException {
        this.protocolVersion = args[0];
        this.peerID = args[1];
        this.accessPoint = args[2];

        this.multicastAddress = "230.0.0.0";
        this.multicastPort = 8080;

        this.group = InetAddress.getByName(this.multicastAddress);
        this.multiSocket = new MulticastSocket(this.multicastPort);
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
        Peer peer = new Peer(args);
        peer.joinMulticast();

        if(args[1].equals("1")) {
            String fileID = peer.makeFileID("test1.txt");
            peer.sendPacket("PUTCHUNK",fileID, "1", "5");


            byte[] pack = new byte[64256];
            DatagramPacket recv = new DatagramPacket(pack, pack.length);
            peer.multiSocket.receive(recv);

            List<String> msg = peer.parseMessage(recv);
            peer.interpretMessage(msg);
        }

        if(args[1].equals("2")) {
            byte[] pack = new byte[64256];
            DatagramPacket recv = new DatagramPacket(pack, pack.length);
            peer.multiSocket.receive(recv);

            List<String> msg = peer.parseMessage(recv);
            peer.interpretMessage(msg);

        }
    }

    private void interpretMessage(List<String> msg) throws NoSuchAlgorithmException, IOException {
        switch(msg.get(1)){
            case "PUTCHUNK":
                System.out.println("PUTCHUNK");
                this.sendPacket("STORED",msg.get(3),msg.get(4),null);
                break;
            case "GETCHUNK":
                System.out.println("GETCHUNK");
                this.sendPacket("CHUNK",msg.get(3),msg.get(4),null);
                break;
            case "DELETE":
                System.out.println("DELETE");
                break;
            case "REMOVED":
                System.out.println("REMOVED");
                break;
            case "STORED":
                System.out.println("STORED");
                break;
            case "CHUNK":
                System.out.println("CHUNK");
                break;

        }

    }


    public void joinMulticast() throws IOException, NoSuchAlgorithmException {
        this.multiSocket.joinGroup(this.group);
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


    private List<String> parseMessage(DatagramPacket recv) {
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

    private void sendPacket(String messageType, String fileID,String chunkNO, String replicationDegree) throws NoSuchAlgorithmException, IOException {
        byte[] header = this.makeHeader(messageType,fileID,chunkNO, replicationDegree);
        byte[] teste = "Teste\0Teste".getBytes();

        int aLen = header.length;
        int bLen = teste.length;
        byte[] result = new byte[aLen + bLen];

        System.arraycopy(header, 0, result, 0, aLen);
        System.arraycopy(teste, 0, result, aLen, bLen);

        DatagramPacket pack = new DatagramPacket(result, result.length, this.group, this.multicastPort);
        this.multiSocket.send(pack);
    }


}
