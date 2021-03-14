import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;


public class Peer {
    String protocolVersion;
    String peerID;
    String accessPoint;

    String multicastAddress;
    Integer multicastPort;

    public void peer(String[] args) throws IOException, NoSuchAlgorithmException {
        this.protocolVersion = args[0];
        this.peerID = args[1];
        this.accessPoint = args[2];

        this.multicastAddress = "230.0.0.0";
        this.multicastPort = 8080;

        InetAddress group = InetAddress.getByName(multicastAddress);
        MulticastSocket multiSocket = new MulticastSocket(multicastPort);
        multiSocket.joinGroup(group);

        byte[] header = makeHeader("PUTCHUNK","test1.txt");
        System.out.println(Arrays.toString(header));

    }

    private byte[] makeHeader(String msgType, String filePath) throws NoSuchAlgorithmException {
        String version = this.protocolVersion;
        String messageType = msgType;
        String senderID = this.peerID;
        String fileID = makeFileID(filePath);
        String chunkNumber = "1";
        String replicationDegree = "5";

        String finish = version + " " + messageType + " " + senderID + " " + fileID + " " + chunkNumber + " " + replicationDegree + " " + '\r' + '\n';
        System.out.println(finish);
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

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        new Peer().peer(args);
    }


}
