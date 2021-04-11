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


public class Peer implements RMI {
    String stateFile;
    String chunkDir;
    String peerDir;
    PeerState state;

    //if after receiving REMOVED the replication degree drops below the desired amount and entry is added to this map
    HashMap<String, Boolean> restore = new HashMap<>();
    //boolean map that shows if a file chunk is being restored
    HashMap<String, Boolean> restoreFile = new HashMap<>();
    //boolean map that shows if the chunk has already been sent during the GETCHUNK message so only one is sent
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

        String multicastControlAddr = args[3];
        String multicastControlPort = args[4];
        String multicastDataAddr = args[5];
        String multicastDataPort = args[6];
        String multicastRecoveryAddr = args[7];
        String multicastRecoveryPort = args[8];


        this.threadPool = Executors.newScheduledThreadPool(15);
        this.backupProtocolThreadPool = Executors.newScheduledThreadPool(5);

        this.controlListener = new Listener(multicastControlAddr,multicastControlPort , this);

        this.dataListener = new Listener(multicastDataAddr,multicastDataPort , this);

        this.recoveryListener = new Listener(multicastRecoveryAddr,multicastRecoveryPort ,this);

        this.peerDir = "peer" + this.peerID;
        this.chunkDir = this.peerDir + "/chunks";
        this.stateFile = this.peerDir + "/state";

        File peer = new File(this.peerDir);
        if (!peer.exists())
            if (!peer.mkdir()) {
                System.out.println("Error creating peer directory");
            }
    }

    private void updateFileChanges()
    {
        try {
            //restores previous state
            FileInputStream fis = new FileInputStream(this.stateFile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.state = (PeerState) ois.readObject();
            fis.close();
            ois.close();

            //when starting the peer, all files are checked and if there are any changes everyone is alerted
            for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
                File f = new File(this.peerDir + "/" + entry.getValue());
                if (!f.exists()) {
                    //file was deleted
                    this.deleteByID(entry.getKey());
                    this.state.filenameToFileID.remove(entry.getKey());
                } else if (!this.makeFileID(entry.getValue()).equals(entry.getKey())) {
                    //file was modified
                    this.deleteByID(entry.getKey());
                    this.backup(entry.getValue(), this.state.desiredRepDegree.get(entry.getKey() + "_0"));
                    this.state.filenameToFileID.remove(entry.getKey());
                }
            }
        } catch (Exception e) {
            System.out.println("No previous state found, starting a new one");
            this.state = new PeerState();
        }
        long currentSize = 0;
        File file = new File(this.chunkDir);
        File[] contents = file.listFiles();

        //updates the peer's current size
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
        //resumes all operations that were stopped
        for (String s : this.state.operations) {
            List<String> operation = Arrays.asList(s.split("-"));
            switch (operation.get(0)) {
                case "backup":
                    this.threadPool.execute(() -> {
                        try {
                            this.backup(operation.get(1), Integer.parseInt(operation.get(2)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case "delete":
                    this.threadPool.execute(() -> {
                        try {
                            this.delete(operation.get(1));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                    break;
                case "restore":
                    this.threadPool.execute(() -> {
                        try {
                            this.restore(0, operation.get(1));
                        } catch (Exception e) {
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
        peer.updateFileChanges();

        //thread that saves the state every second
        peer.threadPool.scheduleAtFixedRate(() -> {
            try {
                peer.updateState();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.SECONDS);

        //connect RMI last in case it doesn't exist
        peer.setupRMI();

    }

    private void updateState() throws IOException {
        //updates state file
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
        String filePath = "peer" + this.peerID + "/" + fileName;
        this.state.operations.add("backup-" + fileName + "-" + ReplicationDegree);
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
        //iterates through a file's chunks
        while ((bytesRead = fileInput.read(pack)) != -1) {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            int finalCurrentChunk = currentChunk;
            //for every chunk a PUTCHUNK message is send

            this.backupProtocolThreadPool.execute(() ->
            {
                try {
                    this.sendPutchunk(fileID, Integer.toString(finalCurrentChunk), ReplicationDegree.toString(), body, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //if the body's length is not 64000, last chunk is found so we can consider the protocol is finished
                if(body.length != 64000)
                    this.state.operations.remove("backup-" + fileName + "-" + ReplicationDegree);
            });
            currentChunk++;
            lastBytesRead = bytesRead;
        }
        //if the last chunk has 64000 bytes, its necessary to send another with an empty body
        if (lastBytesRead == 64000) {
            int finalCurrentChunk1 = currentChunk;
            this.backupProtocolThreadPool.execute(() ->
            {
                try {
                    this.sendPutchunk(fileID, Integer.toString(finalCurrentChunk1), ReplicationDegree.toString(), "".getBytes(), true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                this.state.operations.remove("backup-" + fileName + "-" + ReplicationDegree);
            });
        }
    }

    public void backupLostChunk(String fID, Integer chunkNo) throws Exception {

        //the initiator restores a chunk's replication degree in case a REMOVED message is received
        //and no one else has this chunk
        String fileName = this.state.filenameToFileID.get(fID);
        Integer replicationDegree = this.state.desiredRepDegree.get(fID + "_" + chunkNo.toString());
        String filePath = this.peerDir + "/" + fileName;
        byte[] pack = new byte[64000];
        int bytesRead, currentChunk = 0;
        FileInputStream fileInput = new FileInputStream(filePath);

        //iterates though the file in order to get the correct chunk
        while ((bytesRead = fileInput.read(pack)) != -1 && currentChunk != chunkNo) {
            currentChunk++;
        }
        //sends the PUTCHUNK message
        if (currentChunk == chunkNo) {
            byte[] body = Arrays.copyOfRange(pack, 0, bytesRead);
            this.sendPutchunk(fID, Integer.toString(currentChunk), replicationDegree.toString(), body, true);
        }
        fileInput.close();
    }

    public void restore(String filePath) throws IOException, NoSuchAlgorithmException {
        //starts the file restoration protocol
        String fileID = this.makeFileID(filePath);
        this.state.operations.add("restore-" + fileID);
        this.restoreFile.put(fileID, true);
        this.sendPacket("GETCHUNK", fileID, "0", null, "".getBytes());
    }

    public void restore(Integer chunkNo, String fileID) throws IOException {
        //continues the restoration process
        this.restoreFile.put(fileID, true);
        this.sendPacket("GETCHUNK", fileID, chunkNo.toString(), null, "".getBytes());
    }


    public void deleteByID(String fileID) throws IOException {
        //sends a DELETE when only the fileID is available.
        if (this.state.deletedFilesFromPeers.get(fileID) == null && this.protocolVersion.equals("1.1") && this.state.replicationDegreeMap.get(fileID) != null)
            this.state.deletedFilesFromPeers.put(fileID, this.state.replicationDegreeMap.get(fileID));
        List<String> keysToRemove = new ArrayList<>();

        //removes all chunks of the file from the replication degree maps
        for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
            if (entry.getKey().contains(fileID)) {
                keysToRemove.add(entry.getKey());
            }
        }

        this.state.filenameToFileID.remove(fileID);

        for(String s: keysToRemove){
            this.state.replicationDegreeMap.remove(s);
            this.state.desiredRepDegree.remove(s);
        }


        this.sendPacket("DELETE", fileID, null, null, "".getBytes());
    }

    public void delete(String filePath) throws NoSuchAlgorithmException, IOException {
        //starts the delete protocol

        this.state.operations.add("delete-" + filePath);
        String fileID = this.makeFileID(filePath);

        //creates an entry on the map of peers that need to delete a file if the protocol version is 1.1
        //and removes the file's entries from the other maps
        if (this.state.deletedFilesFromPeers.get(fileID) == null && this.protocolVersion.equals("1.1"))
            for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
                if (this.state.deletedFilesFromPeers.get(fileID) == null) {
                    this.state.deletedFilesFromPeers.put(fileID, entry.getValue());
                } else if (entry.getKey().contains(fileID))
                    this.state.deletedFilesFromPeers.get(fileID).addAll(entry.getValue());
            }

        //removes all chunks of the file from the replication degree maps
        List<String> keysToRemove = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : this.state.replicationDegreeMap.entrySet()) {
            if (entry.getKey().contains(fileID)) {
                keysToRemove.add(entry.getKey());
            }
        }

        for(String s: keysToRemove){
            this.state.replicationDegreeMap.remove(s);
            this.state.desiredRepDegree.remove(s);
        }

        this.state.filenameToFileID.remove(fileID);


        this.sendPacket("DELETE", fileID, null, null, "".getBytes());
        this.state.operations.remove("delete-" + filePath);
    }

    public void reclaim(Integer maxSize) throws Exception {
        this.state.operations.add("reclaim-" + maxSize);
        this.state.maxSize = maxSize;
        List<List<String>> filenameToExcessReplicationDegree = new ArrayList<>();

        //creates a list with every chunk with their corresponding excess replication degree
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

            //orders the files by the order of excess replication degree
            filenameToExcessReplicationDegree.sort(Comparator.comparingInt((List<String> a) -> Integer.parseInt(a.get(1))));
            Collections.reverse(filenameToExcessReplicationDegree);

            //deletes the chunks by the order if excess replication degree
            int currentIndex = 0;
            while (this.state.currentSize > this.state.maxSize && currentIndex < filenameToExcessReplicationDegree.size()) {
                String fileID = filenameToExcessReplicationDegree.get(currentIndex).get(0).split("_")[0];
                String chunkNo = filenameToExcessReplicationDegree.get(currentIndex).get(0).split("_")[1];
                this.sendPacket("REMOVED", fileID, chunkNo, null, "".getBytes());
                this.state.currentSize -= Long.parseLong(filenameToExcessReplicationDegree.get(currentIndex).get(2)) / 1000;
                File toDelete = new File(this.chunkDir + "/" + fileID + "/" + filenameToExcessReplicationDegree.get(currentIndex).get(0));
                if (!toDelete.delete()) {
                    System.out.println("Error deleting chunk");
                }
                currentIndex++;
            }
        }
        this.state.operations.add("reclaim-" + maxSize);

        //restarts data listening function if there is available space
        if (this.state.currentSize < this.state.maxSize && this.state.maxSize != -1 && this.protocolVersion.equals("1.1")) {
            if (!this.dataListener.connect)
                this.dataListener.startThread();
        }
    }

    public String state() {
        // return the peer's state to the test app
        StringBuilder result = new StringBuilder();
        result.append("FILES WHOSE BACKUP I INITIATED\n");
        for (Map.Entry<String, String> entry : this.state.filenameToFileID.entrySet()) {
            result.append("File path: ").append(entry.getValue());
            result.append(" - ");
            result.append("File ID: ").append(entry.getKey());
            result.append(" - ");
            result.append("Desired Replication Degree: ").append(this.state.desiredRepDegree.get(entry.getKey() + "_0"));
            for (Map.Entry<String, Set<String>> entry2 : this.state.replicationDegreeMap.entrySet()) {
                if (entry2.getKey().contains(entry.getKey())) {
                    result.append("\n");
                    result.append("\tChunk number: ").append(entry2.getKey().split("_")[1]);
                    result.append(" - ");
                    result.append("Is saved on the following peers: ").append(entry2.getValue().toString());
                }
            }
        }
        result.append("\nFILES THAT I HAVE BACKED UP\n");
        File file = new File(this.chunkDir);
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                File[] chunks = f.listFiles();
                if (chunks != null) {
                    for (File c : chunks) {
                        result.append("Chunk ID: ").append(c.getName());
                        result.append(" - ");
                        result.append("Chunk Size: ").append(c.length() / 1000).append(" kb");
                        result.append(" - ");
                        result.append("Desired Replication Degree: ").append(this.state.desiredRepDegree.get(c.getName()));
                        result.append(" - ");
                        result.append("Is saved on the following peers: ").append(this.state.replicationDegreeMap.get(c.getName()).toString()).append("\n");
                    }
                }
            }
        }
        result.append("\nSTORAGE CAPACITY: ");
        result.append(this.state.maxSize).append("\n");
        result.append("\nCAPACITY OCCUPIED: ");
        result.append(this.state.currentSize).append("\n");
        return result.toString();
    }

    public void saveFile(String fileID) throws IOException {
        //joins all chunks from a file in order to save it
        File dir = new File(this.chunkDir + "/" + fileID);

        File restoredDir = new File(this.peerDir + "/restored");

        //if the "restored" dir does not exist creats it
        if (!restoredDir.exists())
            restoredDir.mkdirs();


        FileOutputStream fos = new FileOutputStream(this.peerDir + "/restored/" + this.state.filenameToFileID.get(fileID));
        String[] fileNames = dir.list();

        //writes all chunks contents into a file by their chunk order
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

        this.sendPacket("STORED", msg.fileID, msg.chunkNO, null, "".getBytes());
    }

    public void getchunk(Message msg) {
        String filename = this.chunkDir + "/" + msg.fileID + "/" + msg.fileID + "_" + msg.chunkNO;
        try {
            Path filePath = Path.of(filename);
            byte[] body = Files.readAllBytes(filePath);

            if (this.protocolVersion.equals("1.1") && msg.version.equals("1.1")) {
                //if the ServerSocket constructor has a '0', a free port is used
                ServerSocket serverSocket = new ServerSocket(0);
                //gets the port selected and sends it to the initiator peer
                int boundPort = serverSocket.getLocalPort();
                this.sendPacket("CHUNK", msg.fileID, msg.chunkNO, null, Integer.toString(boundPort).getBytes());

                //creates a timeout and connects to the socket. If the timeout is reached the process is stopped
                serverSocket.setSoTimeout(2000);
                Socket clientSocket;
                try {
                    clientSocket = serverSocket.accept();//tries to connect
                    BufferedOutputStream bof = new BufferedOutputStream(clientSocket.getOutputStream());
                    bof.write(body);//writes the body to the socket

                    clientSocket.close();
                    serverSocket.close();
                    bof.close();
                } catch (Exception e) {
                    System.err.println("Noone Connected");
                }

            } else {
                //sends the chunk to the other peer
                this.sendPacket("CHUNK", msg.fileID, msg.chunkNO, null, body);
            }
        } catch (Exception e) {
            System.out.print("Chunk does not exist on this peer's file system ");
            System.out.println(filename);
        }
    }

    public boolean deletechunks(String fileID) {
        //deletes all chunks from a file and updates the current size count.
        //return true if the delition is successful or false if there are no files to delete or if there are any problems
        File file = new File(this.chunkDir + "/" + fileID);
        if (file.exists()) {
            File[] contents = file.listFiles();
            if (contents != null) {
                for (File f : contents) {
                    this.state.replicationDegreeMap.remove(f.getName());
                    this.state.desiredRepDegree.remove(f.getName());
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
        //saves chunk into the chunk folder and updates the current size
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
        //makes a header for the message
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
        //makes a hash with the file path and modification date
        StringBuilder ret = new StringBuilder();
        File f = new File("peer" + this.peerID + "/" + filePath);

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

    public void sendPacket(String messageType, String fileID, String chunkNO, String replicationDegree, byte[] body) throws IOException {
        //gets the message header
        byte[] header = this.makeHeader(messageType, fileID, chunkNO, replicationDegree);

        //joins the message header and the message body
        int headerLen = header.length;
        int bodyLen = body.length;
        byte[] result = new byte[headerLen + bodyLen];

        arraycopy(header, 0, result, 0, headerLen);
        arraycopy(body, 0, result, headerLen, bodyLen);


        //selects the correct multicast channel to send the packet
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


        //if the message is correct the message is sent
        if (socketToSend != null && portToSend != null) {
            DatagramPacket pack = new DatagramPacket(result, result.length, groupToSend, portToSend);
            socketToSend.send(pack);
        }
    }


    public void sendPutchunk(String fileID, String chunkNO, String replicationDegree, byte[] body, boolean createEntry) throws IOException, InterruptedException {
        int nTries = 0;  //counts the number of times the message is sent, if 5 are reaced the operation is stopped
        do {
            //if we wish to create an entry, the replication degree maps are updated with the correct values
            if (createEntry) {
                Set<String> set = new HashSet<>();
                this.state.replicationDegreeMap.put(fileID + "_" + chunkNO, set);
                this.state.desiredRepDegree.put(fileID + "_" + chunkNO, Integer.parseInt(replicationDegree));
            }
            //sends PUTCHUNK message
            this.sendPacket("PUTCHUNK", fileID, chunkNO, replicationDegree, body);
            Thread.sleep((long) Math.pow(2, nTries) * 1000); //waits some time for all STORED messages
            nTries++;
            if (nTries > 5)
                break;
            //continues while a map is null (sometimes the set constructor returns null) or the replication degree is not reached
        } while (this.state.replicationDegreeMap.get(fileID + "_" + chunkNO) == null ||
                this.state.desiredRepDegree.get(fileID + "_" + chunkNO) == null ||
                this.state.replicationDegreeMap.get(fileID + "_" + chunkNO).size() < this.state.desiredRepDegree.get(fileID + "_" + chunkNO));
    }
}
