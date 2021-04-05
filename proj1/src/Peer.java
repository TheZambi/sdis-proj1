import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
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
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;
import static java.lang.System.setOut;


public class Peer implements RMI {
    PeerState state = null;

    HashMap<String, Boolean> restore = new HashMap<>();
    HashMap<String, Boolean> restoreFile = new HashMap<>();
    ScheduledExecutorService threadPool;
    ScheduledExecutorService backupProtocolThreadPool;

    String protocolVersion;
    String peerID;
    String accessPoint;

    Integer lastChunkReceivedSize;

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

        this.lastChunkReceivedSize = 0;

        this.threadPool = Executors.newScheduledThreadPool(15);
        this.backupProtocolThreadPool = Executors.newScheduledThreadPool(10);

        this.controlListener = new Listener(multicastControl, this);

        this.dataListener = new Listener(multicastData, this);

        this.recoveryListener = new Listener(multicastRecovery, this);


        try {
            FileInputStream fis = new FileInputStream("../peer" + this.peerID + "/state.txt");
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.state = (PeerState) ois.readObject();
        }catch (FileNotFoundException e)
        {
            System.out.println("No previous state found, starting a new one");;
            this.state = new PeerState();
        }
    }

    public static void main(String[] args) throws Exception {
        Peer peer = new Peer(args);
        peer.joinMulticast();
        peer.setupRMI();

        peer.threadPool.scheduleAtFixedRate(() ->{
            try {
                peer.updateState();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 1, 5, TimeUnit.SECONDS);

    }

    private void updateState() throws IOException {
        FileOutputStream fos = new FileOutputStream("../peer" + this.peerID + "/state.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(this.state);
    }



//    private void getState() throws IOException, ClassNotFoundException {
//        FileInputStream fis = new FileInputStream("../peer" + this.peerID + "/state.txt");
//        ObjectInputStream ois = new ObjectInputStream(fis);
//        this = (Peer)ois.readObject();
//    }

    private void setupRMI() throws RemoteException, AlreadyBoundException {
        RMI stub = (RMI) UnicastRemoteObject.exportObject(this, 0);
        // Bind the remote object's stub in the registry
        Registry registry = LocateRegistry.getRegistry();
        registry.bind(this.accessPoint, stub);
        System.err.println("Server ready");
    }

    public void restore(String filePath) throws NoSuchAlgorithmException, IOException {
        String fileID = this.makeFileID(filePath);
        this.restoreFile.put(fileID, true);
        this.sendPacket("GETCHUNK", fileID, "0", null, "".getBytes());
    }

    public void delete(String filePath) throws NoSuchAlgorithmException, IOException {
        String fileID = this.makeFileID(filePath);
        this.sendPacket("DELETE", fileID, null, null, "".getBytes());
    }

//    For each file whose backup it has initiated:
    //    The file pathname
    //    The backup service id of the file
    //    The desired replication degree
    //    For each chunk of the file:
    //    Its id
    //    Its perceived replication degree
    public void state() throws RemoteException{
        System.out.println("FILES WHOSE BACKUP I INITIATED");
        for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
            System.out.print("File path: " + entry.getKey());
            System.out.print(" - ");
            System.out.print("File ID: " + entry.getValue());
            System.out.print(" - ");
            System.out.print("Desired Replication Degree: " + this.state.desiredRepDegree.get(entry.getValue() + "_0"));
            for (Map.Entry<String, Set<String>> entry2 : this.state.replicationDegreeMap.entrySet()) {
                if (entry2.getKey().contains(entry.getValue())) {
                    System.out.print("\n");
                    System.out.print("\tChunk number: " + entry2.getKey().split("_")[1]);
                    System.out.print(" - ");
                    System.out.print("Is saved on the following peers: " + entry2.getValue().toString());
                }
            }
        }
        System.out.println("\nFILES THAT I HAVE BACKED UP");
        File file = new File("../peer" + this.peerID + "/chunks");
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                File[] chunks = f.listFiles();
                for(File c : chunks) {
                    System.out.print("Chunk ID: " + c.getName());
                    System.out.print(" - ");
                    System.out.print("Chunk Size: " + file.length() / 1024 + " kb");
                    System.out.print(" - ");
                    System.out.print("Desired Replication Degree: " + this.state.desiredRepDegree.get(c.getName()));
                    System.out.print(" - ");
                    System.out.println("Is saved on the following peers: " + this.state.replicationDegreeMap.get(c.getName()).toString());
                }
            }
        }
    }

    public void backup(String filePath, Integer ReplicationDegree) throws Exception {

        byte[] pack = new byte[64000];
        Integer bytesRead = 0, currentChunk = 0, lastBytesRead = 0;
        FileInputStream fileInput = new FileInputStream(new File(filePath));
        String fileID = this.makeFileID(filePath);
        this.state.filenameToFileID.put(filePath,fileID);
        while ((bytesRead = fileInput.read(pack)) != -1) {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            Integer finalCurrentChunk = currentChunk;
            this.backupProtocolThreadPool.execute(() ->
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
        if (lastBytesRead == 64000) {
            Integer finalCurrentChunk1 = currentChunk;
            this.backupProtocolThreadPool.execute(() ->
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
        } while (this.state.replicationDegreeMap.get(fileID + "_" + chunkNO) == null ||
                (this.state.replicationDegreeMap.get(fileID + "_" + chunkNO).size() < this.state.desiredRepDegree.get(fileID + "_" + chunkNO)));
    }

    public void printMsg(String messageType, String peerID) {
        System.out.println("[Peer " + this.peerID + "] Received message of type " + messageType + " from peer " + peerID);
    }

    public void interpretMessage(Message msg) throws Exception {
        //Ignore own messages
        if (msg.peerID.equals(this.peerID))
            return;

        String fileChunk = msg.fileID + "_" + msg.chunkNO;
        printMsg(msg.messageType, msg.peerID);
        switch (msg.messageType) {
            case "PUTCHUNK":
                this.threadPool.schedule(() -> {
                    try {
                        this.putchunk(msg);
                        this.state.desiredRepDegree.put(fileChunk, Integer.parseInt(msg.replicationDegree));

                        this.state.replicationDegreeMap.put(fileChunk, new HashSet<>());
                        this.state.replicationDegreeMap.get(fileChunk).add(this.peerID);

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
                        this.deletechunks(msg.fileID);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                break;
            case "REMOVED":
                if (this.state.replicationDegreeMap.get(fileChunk) != null && this.state.replicationDegreeMap.get(fileChunk).contains(this.peerID)) {
                    this.updateRepDegreeRemove(msg);
                    if (this.state.replicationDegreeMap.get(fileChunk).size() < this.state.desiredRepDegree.get(fileChunk)) {
                        this.restore.put(fileChunk, true);
                        this.threadPool.schedule(() -> {
                            try {
                                if (this.restore.get(fileChunk)) {
                                    String filename = "../peer" + this.peerID + "/chunks/" + msg.fileID + "/" + fileChunk + ".txt";
                                    Path filePath = Path.of(filename);
                                    byte[] body = Files.readAllBytes(filePath);

                                    this.sendPutchunk(msg.fileID, msg.chunkNO, this.state.desiredRepDegree.get(fileChunk).toString(), body);
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, new Random().nextInt(400), TimeUnit.MILLISECONDS);
                    }
                }
                break;
            case "STORED":
                this.updateRepDegreeAdd(msg);
                break;
            case "CHUNK":
                this.lastChunkReceivedSize = msg.body.length;
                this.saveChunk(msg.fileID, msg.chunkNO, msg.body);
                Integer chunkNO = Integer.parseInt(msg.chunkNO) + 1;
                if (this.restoreFile.get(msg.fileID) != null) {
                    if (lastChunkReceivedSize != 64000) {
                        this.restoreFile.put(msg.fileID, false);
                        this.saveFile(msg.fileID);
                    }
                    else if (this.restoreFile.get(msg.fileID)) {
                        this.sendPacket("GETCHUNK", msg.fileID, chunkNO.toString(), null, "".getBytes());
                    }
                }
                break;

        }
    }

    private void saveFile(String fileID) throws IOException {
        File dir = new File("../peer" + this.peerID + "/chunks/"+ fileID);

        FileOutputStream fos = new FileOutputStream("../peer" + this.peerID + "/output.txt");
        String[] fileNames = dir.list();
        Arrays.sort(fileNames, Comparator.comparingInt((String a) -> Integer.parseInt(a.split("_")[1].split("\\.")[0])));

        for (String fileName : fileNames) {
            FileInputStream fis = new FileInputStream("../peer" + this.peerID + "/chunks/"+ fileID+"/"+fileName);
            fis.transferTo(fos);
            fis.close();
        }
        fos.close();

        this.deletechunks(fileID);

    }

    private void updateRepDegreeAdd(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;

        if (this.state.replicationDegreeMap.get(file) != null) {
            this.state.replicationDegreeMap.get(file).add(msg.peerID);
        }
    }

    private void updateRepDegreeRemove(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;
        if (this.state.replicationDegreeMap.get(file) != null)
            this.state.replicationDegreeMap.get(file).remove(msg.peerID);
    }

    private void deletechunks(String fileID) {

        File file = new File("../peer" + this.peerID + "/chunks/" + fileID);
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                f.delete();
            }
        }
        file.delete();
    }

    private void getchunk(Message msg) {
        String filename = "../peer" + this.peerID + "/chunks/" + msg.fileID + "/" + msg.fileID + "_" + msg.chunkNO;
        try {
            Path filePath = Path.of(filename);
            byte[] body = Files.readAllBytes(filePath);
            this.sendPacket("CHUNK", msg.fileID, msg.chunkNO, null, body);
        } catch (Exception e) {
            System.out.println("Chunk does not exist on this peer's file system");
            System.out.println(filename);
        }
    }

    private void putchunk(Message msg) throws Exception {
        saveChunk(msg.fileID, msg.chunkNO, msg.body);

        this.sendPacket("STORED", msg.fileID, msg.chunkNO, null, "".getBytes());
    }

    private void saveChunk(String fileID, String chunkNO, byte[] body) throws IOException {
        //fileID_chunkNO.txt
        File dir = new File("../peer" + this.peerID + "/chunks/" + fileID);

        if (!dir.exists()) {
            dir.mkdirs();
        }

        String filename = dir + "/" + fileID + "_" + chunkNO;


        FileOutputStream fos = new FileOutputStream(filename);

        fos.write(body);

        fos.close();
    }

    public void joinMulticast() throws Exception {
        this.controlListener.startThread();
        this.dataListener.startThread();
        this.recoveryListener.startThread();
    }

    private byte[] makeHeader(String msgType, String fID, String chunkNO, String repDegree) {
        String version = this.protocolVersion;
        String senderID = this.peerID;
        String chunkNumber = "";
        if (chunkNO != null)
            chunkNumber = " " + chunkNO;
        String replicationDegree = "";
        if (repDegree != null)
            replicationDegree = " " + repDegree;

        String finish = version + " " + msgType + " " + senderID + " " + fID + chunkNumber + replicationDegree + " " + '\r' + '\n' + '\r' + '\n';
        return finish.getBytes();
    }

    private String makeFileID(String filePath) throws NoSuchAlgorithmException {
        StringBuilder ret = new StringBuilder();
        File f = new File(filePath);
        String absPath = f.getAbsolutePath();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(absPath.getBytes());
        for (byte b : hash) {
            ret.append(String.format("%02x", b));
        }
        return ret.toString();
    }

    private void sendPacket(String messageType, String fileID, String chunkNO, String replicationDegree, byte[] body) throws IOException {
        byte[] header = this.makeHeader(messageType, fileID, chunkNO, replicationDegree);

        int aLen = header.length;
        int bLen = body.length;
        byte[] result = new byte[aLen + bLen];

        arraycopy(header, 0, result, 0, aLen);
        arraycopy(body, 0, result, aLen, bLen);


        InetAddress groupToSend = null;
        Integer portToSend = null;
        MulticastSocket socketToSend = null;
        switch (messageType) {
            case "PUTCHUNK":
                groupToSend = this.dataListener.group;
                portToSend = this.dataListener.port;
                socketToSend = this.dataListener.socket;
                Set<String> set = new HashSet<>();
                this.state.replicationDegreeMap.put(fileID + "_" + chunkNO, set);
                this.state.desiredRepDegree.put(fileID + "_" + chunkNO, Integer.parseInt(replicationDegree));
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
