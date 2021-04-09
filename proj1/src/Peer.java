import java.io.*;
import java.net.*;
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
    String stateFile;
    String chunkDir;
    String peerDir;
    PeerState state;

    HashMap<String, Boolean> restore = new HashMap<>();
    HashMap<String, Boolean> restoreFile = new HashMap<>();
    HashMap<String, Boolean> getChunkMap = new HashMap<>();
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
        this.backupProtocolThreadPool = Executors.newScheduledThreadPool(5);

        this.controlListener = new Listener(multicastControl, this);

        this.dataListener = new Listener(multicastData, this);

        this.recoveryListener = new Listener(multicastRecovery, this);

        this.peerDir = "../peer" + this.peerID;
        this.chunkDir = this.peerDir + "/chunks";
        this.stateFile = this.peerDir + "/state";

        File peer = new File(this.peerDir);
        if (!peer.exists())
            if (!peer.mkdir()) {
                System.out.println("Error creating peer directory");
                return;
            }

        try {
            FileInputStream fis = new FileInputStream(this.stateFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.state = (PeerState) ois.readObject();
            fis.close();
            ois.close();
            for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
                File f = new File(this.peerDir + "/" + entry.getValue());
                if (!f.exists()) {
                    System.out.println("Deleted File");
                    this.deleteByID(entry.getKey());
                    this.state.filenameToFileID.remove(entry.getKey());
                } else if (!this.makeFileID(entry.getValue()).equals(entry.getKey())) {
                    System.out.println("Changed File");
                    this.deleteByID(entry.getKey());
                    this.backup(entry.getValue(), this.state.desiredRepDegree.get(entry.getKey() + "_0"));
                    this.state.filenameToFileID.remove(entry.getKey());
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("No previous state found, starting a new one");
            this.state = new PeerState();
        }

        long currentSize = 0;
        File file = new File(this.chunkDir);
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                File[] chunks = f.listFiles();
                if (chunks != null) {
                    for (File c : chunks) {
                        currentSize += c.length() / 1000;
                    }
                }
            }
        }
        this.state.currentSize = currentSize;
        this.resumeOperations();
    }

    private void resumeOperations() {
        for (String s : this.state.operations) {
            List<String> operation = Arrays.asList(s.split("-"));
            switch (operation.get(0)) {
                case "backup":
                    this.threadPool.execute(() -> {
                        try {
                            this.backup(this.state.filenameToFileID.get(operation.get(1)), Integer.parseInt(operation.get(2)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case "delete":
                    this.threadPool.execute(() -> {
                        try {
                            this.delete(operation.get(1));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case "restore":
                    this.threadPool.execute(() -> {
                        try {
                            this.restore(0, operation.get(1));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case "reclaim":
                    this.threadPool.execute(() -> {
                        try {
                            this.reclaim(Integer.parseInt(operation.get(1)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Peer peer = new Peer(args);
        peer.joinMulticast();
        peer.setupRMI();

        peer.threadPool.scheduleAtFixedRate(() -> {
            try {
                peer.updateState();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.SECONDS);

    }

    private void updateState() throws IOException {
        FileOutputStream fos = new FileOutputStream(this.stateFile);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(this.state);
        oos.close();
        fos.close();

    }

    private void setupRMI() throws RemoteException, AlreadyBoundException {
        RMI stub = (RMI) UnicastRemoteObject.exportObject(this, 0);
        // Bind the remote object's stub in the registry
        Registry registry = LocateRegistry.getRegistry();
        registry.bind(this.accessPoint, stub);
        System.err.println("Server ready");
    }

    public void joinMulticast() throws Exception {
        this.controlListener.startThread();
        this.dataListener.startThread();
        this.recoveryListener.startThread();
    }


    public void backup(String fileName, Integer ReplicationDegree) throws Exception {
        String filePath = "../peer" + this.peerID + "/" + fileName;
        byte[] pack = new byte[64000];
        int bytesRead, currentChunk = 0, lastBytesRead = 0;
        FileInputStream fileInput = new FileInputStream(new File(filePath));
        String fileID = this.makeFileID(fileName);

        //Removes Previous File If Changed
        for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
            if (!fileID.equals(entry.getKey()) && entry.getValue().equals(fileName)) {
                System.out.println("Changed File");
                this.deleteByID(entry.getKey());
                this.state.filenameToFileID.remove(entry.getKey());
            }
        }


        this.state.filenameToFileID.put(fileID, fileName);
        while ((bytesRead = fileInput.read(pack)) != -1) {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            int finalCurrentChunk = currentChunk;
            this.backupProtocolThreadPool.execute(() ->
            {
                try {
                    this.sendPutchunk(fileID, Integer.toString(finalCurrentChunk), ReplicationDegree.toString(), body, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            currentChunk++;
            lastBytesRead = bytesRead;
        }
        if (lastBytesRead == 64000) {
            int finalCurrentChunk1 = currentChunk;
            this.backupProtocolThreadPool.execute(() ->
            {
                try {
                    this.sendPutchunk(fileID, Integer.toString(finalCurrentChunk1), ReplicationDegree.toString(), "".getBytes(), true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void backupLostChunk(String fID, Integer chunkNo) throws Exception {
        String fileName = this.state.filenameToFileID.get(fID);
        Integer replicationDegree = this.state.desiredRepDegree.get(fID + "_" + chunkNo.toString());
        String filePath = this.peerDir + "/" + fileName;
        byte[] pack = new byte[64000];
        int bytesRead, currentChunk = 0;
        FileInputStream fileInput = new FileInputStream(filePath);
        while ((bytesRead = fileInput.read(pack)) != -1 && currentChunk != chunkNo) {
            currentChunk++;
        }
        if (currentChunk == chunkNo) {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            int finalCurrentChunk = currentChunk;
            this.sendPutchunk(fID, Integer.toString(finalCurrentChunk), replicationDegree.toString(), body, true);
        }
        fileInput.close();
    }

    public void restore(String filePath) throws NoSuchAlgorithmException, IOException {

        String fileID = "";
        for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
            if (entry.getValue().equals(filePath)) {
                fileID = entry.getKey();
                break;
            }
        }

        if (fileID.equals("")) {
            System.out.println("File is not backed up!");
            return;
        }

        this.state.operations.add("restore-" + fileID);
        this.restoreFile.put(fileID, true);
        this.sendPacket("GETCHUNK", fileID, "0", null, "".getBytes(), false);
    }

    public void restore(Integer chunkNo, String fileID) throws NoSuchAlgorithmException, IOException {
        this.restoreFile.put(fileID, true);
        this.sendPacket("GETCHUNK", fileID, chunkNo.toString(), null, "".getBytes(), false);
    }


    public void deleteByID(String fileID) throws NoSuchAlgorithmException, IOException {
        if (this.state.deletedFilesFromPeers.get(fileID) == null && this.protocolVersion.equals("1.1") && this.state.replicationDegreeMap.get(fileID) != null)
            this.state.deletedFilesFromPeers.put(fileID, this.state.replicationDegreeMap.get(fileID));
        for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
            if (entry.getKey().contains(fileID)) {
                this.state.replicationDegreeMap.remove(entry.getKey());
                this.state.desiredRepDegree.remove(entry.getKey());
            }
        }

        this.sendPacket("DELETE", fileID, null, null, "".getBytes(), false);
    }

    public void delete(String filePath) throws NoSuchAlgorithmException, IOException {
        this.state.operations.add("delete-" + filePath);
        String fileID = this.makeFileID(filePath);
        if (this.state.deletedFilesFromPeers.get(fileID) == null && this.protocolVersion.equals("1.1"))
            for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
                if (this.state.deletedFilesFromPeers.get(fileID) == null) {
                    this.state.deletedFilesFromPeers.put(fileID, entry.getValue());
                } else if (entry.getKey().contains(fileID))
                    this.state.deletedFilesFromPeers.get(fileID).addAll(entry.getValue());
            }
        for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
            if (entry.getKey().contains(fileID)) {
                this.state.replicationDegreeMap.remove(entry.getKey());
                this.state.desiredRepDegree.remove(entry.getKey());
            }
        }
        this.sendPacket("DELETE", fileID, null, null, "".getBytes(), false);
        this.state.operations.remove("delete-" + filePath);
    }

    public void reclaim(Integer maxSize) throws Exception {
        this.state.operations.add("reclaim-" + maxSize);
        this.state.maxSize = maxSize;
        List<List<String>> filenameToExcessReplicationDegree = new ArrayList<>();
        if (this.state.currentSize > this.state.maxSize) {
            File file = new File(this.chunkDir);
            File[] contents = file.listFiles();
            if (contents != null) {
                for (File f : contents) {
                    File[] chunks = f.listFiles();
                    if (chunks != null) {
                        for (File c : chunks) {
                            int excessRepDegree = this.state.replicationDegreeMap.get(c.getName()).size() - this.state.desiredRepDegree.get(c.getName());
                            List<String> aux = new ArrayList<>();
                            aux.add(c.getName());
                            aux.add(Integer.toString(excessRepDegree));
                            aux.add(String.valueOf(c.length()));
                            filenameToExcessReplicationDegree.add(aux);
                        }
                    }
                }
            }

            filenameToExcessReplicationDegree.sort(Comparator.comparingInt((List<String> a) -> Integer.parseInt(a.get(1))));
            Collections.reverse(filenameToExcessReplicationDegree);

            int currentIndex = 0;
            while (this.state.currentSize > this.state.maxSize && currentIndex < filenameToExcessReplicationDegree.size()) {
                String fileID = filenameToExcessReplicationDegree.get(currentIndex).get(0).split("_")[0];
                String chunkNo = filenameToExcessReplicationDegree.get(currentIndex).get(0).split("_")[1];
                this.sendPacket("REMOVED", fileID, chunkNo, null, "".getBytes(), false);
                this.state.currentSize -= Long.parseLong(filenameToExcessReplicationDegree.get(currentIndex).get(2)) / 1000;
                File toDelete = new File(this.chunkDir + "/" + fileID + "/" + filenameToExcessReplicationDegree.get(currentIndex).get(0));
                if (!toDelete.delete()) {
                    System.out.println("Error deleting chunk");
                }
                currentIndex++;
            }
        }
        this.state.operations.add("reclaim-" + maxSize);
        if (this.state.currentSize < this.state.maxSize && this.state.maxSize != -1 && this.protocolVersion.equals("1.1")) {
            if (!this.dataListener.connect)
                this.dataListener.startThread();
        }
    }

    public String state() {
        String result = "";
        result += "FILES WHOSE BACKUP I INITIATED\n";
        for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
            result += "File path: " + entry.getValue();
            result += " - ";
            result += "File ID: " + entry.getKey();
            result += " - ";
            result += "Desired Replication Degree: " + this.state.desiredRepDegree.get(entry.getKey() + "_0");
            for (Map.Entry<String, Set<String>> entry2 : this.state.replicationDegreeMap.entrySet()) {
                if (entry2.getKey().contains(entry.getKey())) {
                    result += "\n";
                    result += "\tChunk number: " + entry2.getKey().split("_")[1];
                    result += " - ";
                    result += "Is saved on the following peers: " + entry2.getValue().toString();
                }
            }
        }
        result += "\nFILES THAT I HAVE BACKED UP\n";
        File file = new File(this.chunkDir);
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                File[] chunks = f.listFiles();
                if (chunks != null) {
                    for (File c : chunks) {
                        result += "Chunk ID: " + c.getName();
                        result += " - ";
                        result += "Chunk Size: " + c.length() / 1000 + " kb";
                        result += " - ";
                        result += "Desired Replication Degree: " + this.state.desiredRepDegree.get(c.getName());
                        result += " - ";
                        result += "Is saved on the following peers: " + this.state.replicationDegreeMap.get(c.getName()).toString();
                    }
                }
            }
        }
        result += "\nSTORAGE CAPACITY: ";
        result += this.state.maxSize + "\n";
        result += "\nCAPACITY OCCUPIED: ";
        result += this.state.currentSize + "\n";
        return result;
    }

    public void saveFile(String fileID) throws IOException {
        File dir = new File(this.chunkDir + "/" + fileID);

        File restoredDir = new File(this.peerDir + "/restored");

        if (!restoredDir.exists())
            restoredDir.mkdirs();

        FileOutputStream fos = new FileOutputStream(this.peerDir + "/restored/" + this.state.filenameToFileID.get(fileID));
        String[] fileNames = dir.list();
        if (fileNames != null) {
            Arrays.sort(fileNames, Comparator.comparingInt((String a) -> Integer.parseInt(a.split("_")[1].split("\\.")[0])));

            for (String fileName : fileNames) {
                FileInputStream fis = new FileInputStream(this.chunkDir + "/" + fileID + "/" + fileName);
                fis.transferTo(fos);
                fis.close();
            }
        }
        fos.close();
        this.deletechunks(fileID);

    }

    public void updateRepDegreeAdd(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;
        if (this.state.replicationDegreeMap.get(file) != null) {
            this.state.replicationDegreeMap.get(file).add(msg.peerID);
        }

    }

    public void updateRepDegreeRemove(Message msg) {
        String file = msg.fileID + "_" + msg.chunkNO;
        if (this.state.replicationDegreeMap.get(file) != null)
            this.state.replicationDegreeMap.get(file).remove(msg.peerID);

    }

    //SUBPROTOCOLS
    public void putchunk(Message msg) throws Exception {
        saveChunk(msg.fileID, msg.chunkNO, msg.body);

        this.sendPacket("STORED", msg.fileID, msg.chunkNO, null, "".getBytes(), false);
    }

    public void getchunk(Message msg) {
        String filename = this.chunkDir + "/" + msg.fileID + "/" + msg.fileID + "_" + msg.chunkNO;
        try {
            Path filePath = Path.of(filename);
            byte[] body = Files.readAllBytes(filePath);

            if (this.protocolVersion.equals("1.1") && msg.version.equals("1.1")) {
                ServerSocket serverSocket = new ServerSocket(0);
                int boundPort = serverSocket.getLocalPort();
                this.sendPacket("CHUNK", msg.fileID, msg.chunkNO, null, Integer.toString(boundPort).getBytes(), false);
                serverSocket.setSoTimeout(2000);
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    BufferedOutputStream bof = new BufferedOutputStream(clientSocket.getOutputStream());
                    bof.write(body);

                    clientSocket.close();
                    serverSocket.close();
                    bof.close();
                } catch (Exception e) {
                    System.err.println("Noone Connected");
                }

            } else {
                this.sendPacket("CHUNK", msg.fileID, msg.chunkNO, null, body, false);
            }
        } catch (Exception e) {
            System.out.print("Chunk does not exist on this peer's file system ");
            System.out.println(filename);
        }
    }

    public boolean deletechunks(String fileID) {
        File file = new File(this.chunkDir + "/" + fileID);
        if (file.exists()) {
            File[] contents = file.listFiles();
            if (contents != null) {
                for (File f : contents) {
                    this.state.currentSize -= f.length() / 1000;
                    if (!f.delete()) {
                        System.out.println("Error deleting chunk file");
                        return false;
                    }
                }
            }
            if (!file.delete())
                System.out.println("Error deleting chunk folder");
            return true;
        }
        return false;
    }

    public void saveChunk(String fileID, String chunkNO, byte[] body) throws IOException {
        File dir = new File(this.chunkDir + "/" + fileID);

        String filename = dir + "/" + fileID + "_" + chunkNO;

        if (!(new File(filename).exists()))
            this.state.currentSize += (body.length / 1000);

        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                System.out.println("Error creating chunk folder");
            }
        }


        FileOutputStream fos = new FileOutputStream(filename);
        fos.write(body);
        fos.close();
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
        File f = new File("../peer" + this.peerID + "/" + filePath);

        //DATA
        String absPath = f.getAbsolutePath();
        String lastModified = String.valueOf(f.lastModified());
        String result = absPath + lastModified;

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(result.getBytes());
        for (byte b : hash) {
            ret.append(String.format("%02x", b));
        }
        return ret.toString();
    }

    public void sendPacket(String messageType, String fileID, String chunkNO, String replicationDegree, byte[] body, boolean createEntry) throws IOException {
        byte[] header = this.makeHeader(messageType, fileID, chunkNO, replicationDegree);

        int headerLen = header.length;
        int bodyLen = body.length;
        byte[] result = new byte[headerLen + bodyLen];

        arraycopy(header, 0, result, 0, headerLen);
        arraycopy(body, 0, result, headerLen, bodyLen);


        InetAddress groupToSend = null;
        Integer portToSend = null;
        MulticastSocket socketToSend = null;
        switch (messageType) {
            case "PUTCHUNK":
                groupToSend = this.dataListener.group;
                portToSend = this.dataListener.port;
                socketToSend = this.dataListener.socket;

                break;
            case "GETCHUNK":
            case "DELETE":
            case "REMOVED":
            case "DELETESUCESS":
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


        if (socketToSend != null && portToSend != null) {
            DatagramPacket pack = new DatagramPacket(result, result.length, groupToSend, portToSend);
            socketToSend.send(pack);
        }
    }

    public void sendPutchunk(String fileID, String chunkNO, String replicationDegree, byte[] body, boolean createEntry) throws IOException, InterruptedException {
        int nTries = 0;
        do {
            if (createEntry) {
                Set<String> set = new HashSet<>();
                this.state.replicationDegreeMap.put(fileID + "_" + chunkNO, set);
                this.state.desiredRepDegree.put(fileID + "_" + chunkNO, Integer.parseInt(replicationDegree));
            }
            this.sendPacket("PUTCHUNK", fileID, chunkNO, replicationDegree, body, createEntry);
            Thread.sleep((long) Math.pow(2, nTries) * 1000);
            nTries++;
            if (nTries > 5)
                break;
        } while (this.state.replicationDegreeMap.get(fileID + "_" + chunkNO) == null ||
                this.state.desiredRepDegree.get(fileID + "_" + chunkNO) == null ||
                this.state.replicationDegreeMap.get(fileID + "_" + chunkNO).size() < this.state.desiredRepDegree.get(fileID + "_" + chunkNO));
    }
}
