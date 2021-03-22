import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Peer {
    String protocolVersion;
    String peerID;
    String accessPoint;
    
    InetAddress controlGroup;
    Integer controlPort;
    MulticastSocket controlSocket;
    
    InetAddress dataGroup;
    Integer dataPort;
    MulticastSocket dataSocket;

    InetAddress recoveryGroup;
    Integer recoveryPort;
    MulticastSocket recoverySocket;

    public Peer(String[] args) throws IOException, NoSuchAlgorithmException {
        this.protocolVersion = args[0];
        this.peerID = args[1];
        this.accessPoint = args[2];

        String multicastControl = args[3];
        String multicastData = args[4];
        String multicastRecovery = args[5];

        this.controlGroup = InetAddress.getByName(multicastControl.split(":")[0]);
        this.controlPort = Integer.parseInt(multicastControl.split(":")[1]);
        this.controlSocket = new MulticastSocket(this.controlPort);


        this.dataGroup = InetAddress.getByName(multicastData.split(":")[0]);
        this.dataPort = Integer.parseInt(multicastData.split(":")[1]);
        this.dataSocket = new MulticastSocket(this.dataPort);


        this.recoveryGroup = InetAddress.getByName(multicastRecovery.split(":")[0]);
        this.recoveryPort = Integer.parseInt(multicastRecovery.split(":")[1]);
        this.recoverySocket = new MulticastSocket(this.recoveryPort);
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        Peer peer = new Peer(args);
        peer.joinMulticast();

        if(args[1].equals("1")) {
            String fileID = peer.makeFileID("test1.txt");

            Path fileName = Path.of("test1.txt");
            String body = Files.readString(fileName);

            peer.sendPacket("DELETE",fileID, "1", "5",body);
        }
    }

    private void interpretMessage(List<String> msg) throws NoSuchAlgorithmException, IOException {
        //Ignore own messages
        if(msg.get(2).equals(this.peerID))
            return;

        switch(msg.get(1)){
            case "PUTCHUNK":
                System.out.println("PUTCHUNK");
                this.putchunk(msg);
                break;
            case "GETCHUNK":
                System.out.println("GETCHUNK");
                this.getchunk(msg);
                break;
            case "DELETE":
                System.out.println("DELETE");
                this.deletechunk(msg);
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

    private void deletechunk(List<String> msg) {
        //Current pwd
        Path currentRelativePath = Paths.get("");
        String path = currentRelativePath.toAbsolutePath().toString();

        String filename = msg.get(3);
        File f = new File(path);
        String[] pathnames;
        pathnames = f.list();
        if(pathnames != null) {
            for (String pathname : pathnames) {
                if (pathname.contains(msg.get(3))) {
                    File file = new File(pathname);
                    file.delete();
                }
            }
        }
    }

    private void getchunk(List<String> msg) throws IOException, NoSuchAlgorithmException {
        String filename = msg.get(3)+"_"+msg.get(4)+".txt";
        try{
            Path filePath = Path.of(filename);
            String body = Files.readString(filePath);
            this.sendPacket("CHUNK",msg.get(3),msg.get(4),null,body);
        }
        catch (Exception e){
            System.out.println("Chunk does not exist on this peer's file system");
        }
    }

    private void putchunk(List<String> msg) throws IOException, NoSuchAlgorithmException {
        //fileID_chunkNO.txt
        String filename = msg.get(3)+"_"+msg.get(4)+".txt";
        File file = new File(filename);
        if (file.createNewFile()) {
            FileWriter writer = new FileWriter(filename);
            writer.write(msg.get(6));
            writer.close();
        }
        this.sendPacket("STORED",msg.get(3),msg.get(4),null,"");

    }

    public void joinMulticast() throws IOException {
        this.controlSocket.joinGroup(this.controlGroup);
        this.dataSocket.joinGroup(this.dataGroup);
        this.recoverySocket.joinGroup(this.recoveryGroup);

        Thread control = new Thread(() -> {
            byte[] pack = new byte[64256];
            DatagramPacket recv = new DatagramPacket(pack, pack.length);
            try {
                this.controlSocket.receive(recv);
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<String> msg = this.parseMessage(recv);
            try {
                this.interpretMessage(msg);
            } catch (Exception e) {
                e.printStackTrace();
        }});


        Thread data = new Thread(() -> {
            byte[] pack = new byte[64256];
            DatagramPacket recv = new DatagramPacket(pack, pack.length);
            try {
                this.dataSocket.receive(recv);
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<String> msg = this.parseMessage(recv);
            try {
                this.interpretMessage(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread recovery = new Thread(() -> {
            byte[] pack = new byte[64256];
            DatagramPacket recv = new DatagramPacket(pack, pack.length);
            try {
                this.recoverySocket.receive(recv);
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<String> msg = this.parseMessage(recv);
            try {
                this.interpretMessage(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        control.start();
        data.start();
        recovery.start();
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
                groupToSend = this.dataGroup;
                portToSend = this.dataPort;
                socketToSend = this.dataSocket;
                break;
            case "GETCHUNK":
                groupToSend = this.controlGroup;
                portToSend = this.controlPort;
                socketToSend = this.controlSocket;
                break;
            case "DELETE":
                groupToSend = this.controlGroup;
                portToSend = this.controlPort;
                socketToSend = this.controlSocket;
                break;
            case "REMOVED":
                groupToSend = this.controlGroup;
                portToSend = this.controlPort;
                socketToSend = this.controlSocket;
                break;
            case "STORED":
                groupToSend = this.controlGroup;
                portToSend = this.controlPort;
                socketToSend = this.controlSocket;
                break;
            case "CHUNK":
                groupToSend = this.recoveryGroup;
                portToSend = this.recoveryPort;
                socketToSend = this.recoverySocket;
                break;

        }

        DatagramPacket pack = new DatagramPacket(result, result.length, groupToSend, portToSend);
        socketToSend.send(pack);
    }


}
