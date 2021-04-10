import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;

public class MessageInterpreter {
    Peer peer;

    MessageInterpreter(Peer peer){
        this.peer = peer;
    }

    public void printMsg(String messageType, String peerID) {
        System.out.println("[Peer " + this.peer.peerID + "] Received message of type " + messageType + " from peer " + peerID);
    }
    
    public void interpretMessage(Message msg) throws Exception {
        synchronized (this.peer.state) {
            //Ignore own messages
            if (msg.peerID.equals(this.peer.peerID))
                return;

            if(this.peer.protocolVersion.equals("1.1")) {
                // If someone didn't delete the files its checked here and another DELETE message is sent
                this.peer.threadPool.execute(() -> {
                    try {
                        this.checkForMissedPeerDelete(msg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            String fileChunk = msg.fileID + "_" + msg.chunkNO;
            this.printMsg(msg.messageType, msg.peerID);
            switch (msg.messageType) {
                case "PUTCHUNK":
                    this.processPutChunk(fileChunk, msg);
                    break;
                case "GETCHUNK":
                    this.processGetChunk(msg);
                    break;
                case "DELETE":
                    this.processDeleteChunk(msg);
                    break;
                case "REMOVED":
                    this.processRemoveChunk(fileChunk, msg);
                    break;
                case "STORED":
                    this.peer.updateRepDegreeAdd(msg);
                    break;
                case "CHUNK":
                    this.processChunk(msg);
                    break;
                case "DELETESUCESS":
                    if(this.peer.protocolVersion.equals("1.1"))
                        this.processDeleteSucess(msg);
                    break;

            }
        }
    }

    private void checkForMissedPeerDelete(Message msg) throws IOException {
        for(Map.Entry<String, Set<String>> entry: this.peer.state.deletedFilesFromPeers.entrySet()){
            if(entry.getValue().contains(msg.peerID)){
                this.peer.sendPacket("DELETE", entry.getKey(), null, null, "".getBytes());
            }
            if(msg.version.equals("1.0")){
                this.peer.state.deletedFilesFromPeers.get(entry.getKey()).remove(msg.peerID);
            }
        }
    }

    //used in protocol version 1.1, removes per from deletedFilesFromPeers map
    private void processDeleteSucess(Message msg) {
        if(this.peer.state.deletedFilesFromPeers.get(msg.fileID) != null) {
            this.peer.state.deletedFilesFromPeers.get(msg.fileID).remove(msg.peerID);
            if (this.peer.state.deletedFilesFromPeers.get(msg.fileID).isEmpty()) {
                this.peer.state.deletedFilesFromPeers.remove(msg.fileID);
            }
        }
    }

    private void processChunk(Message msg) throws IOException {
        this.peer.getChunkMap.put(msg.fileID+"_"+msg.chunkNO,false);
        int chunkNO = Integer.parseInt(msg.chunkNO) + 1;
        if (this.peer.restoreFile.get(msg.fileID) != null) {
            byte[] body;


            if(this.peer.protocolVersion.equals("1.1")
                    & msg.version.equals("1.1")) {
                int nTries = 0;
                Socket socket = null;

                //tries to connect to the other peer 5 times before giving up
                while(nTries<5) {
                    try {
                        //gets the socket to communicate with the peer
                        socket = new Socket(msg.address, Integer.parseInt(new String(msg.body)));
                        Thread.sleep(100);
                        break;
                    }catch (Exception e) {
                        nTries++;
                    }
                }
                if(nTries==5)
                    return;

                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                byte[] buf = new byte[64000];
                byte[] aux = new byte[64000];
                int bytesRead, totalBytesRead = 0;
                //since read can't read 64000 bytes, this stacks them in an auxiliary array to copy to the body
                //in order to keep the total bytes read intact.
                while((bytesRead = in.read(buf))!=-1)
                {
                    arraycopy(buf, 0, aux, totalBytesRead, bytesRead);
                    totalBytesRead+=bytesRead;
                }

                //copies the auxiliary array to the body array
                body = new byte[totalBytesRead];
                arraycopy(aux,0,body,0,totalBytesRead);
                in.close();
                socket.close();
            }
            else{ //if any version is 1.0
                body = msg.body;
            }

            this.peer.saveChunk(msg.fileID, msg.chunkNO, body);
            if (body.length != 64000) {
                //last chunk obtained
                this.peer.restoreFile.put(msg.fileID, false); //restore file is no longer necessary
                this.peer.saveFile(msg.fileID);//joins all chunks
                this.peer.state.operations.remove("restore-" + msg.fileID);
                this.peer.restoreFile.remove(msg.fileID);//removes file from restore map
            }
            else if (this.peer.restoreFile.get(msg.fileID)) {
                this.peer.restore(chunkNO,msg.fileID); // calls the restore protocol for the next chunk
            }
        }
    }

    private void processRemoveChunk(String fileChunk, Message msg) {
        //REMOVE FROM MAP THE PEER WHO REMOVED CHUNK
            if (this.peer.state.replicationDegreeMap.get(fileChunk) != null) {
                this.peer.updateRepDegreeRemove(msg);
                //IF I HAVE THE REMOVED CHUNK
                if (this.peer.state.replicationDegreeMap.get(fileChunk).contains(this.peer.peerID)) {
                    //FELL BELOW REP DEGREE
                    if (this.peer.state.replicationDegreeMap.get(fileChunk).size() < this.peer.state.desiredRepDegree.get(fileChunk)) {
                        this.peer.restore.put(fileChunk, true);
                        this.peer.threadPool.schedule(() -> {
                            try {
                                //IF I AM THE ONE CALLING PUTCHUNK
                                if (this.peer.restore.get(fileChunk)) {
                                    String filename = this.peer.chunkDir + "/" + msg.fileID + "/" + fileChunk;
                                    Path filePath = Path.of(filename);
                                    byte[] body = Files.readAllBytes(filePath);

                                    this.peer.sendPutchunk(msg.fileID, msg.chunkNO, this.peer.state.desiredRepDegree.get(fileChunk).toString(), body, false);
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, new Random().nextInt(400), TimeUnit.MILLISECONDS);
                    }
                }
                else if(this.peer.state.filenameToFileID.get(msg.fileID) != null){
                    if(this.peer.state.replicationDegreeMap.get(fileChunk).size() == 0){
                        this.peer.backupProtocolThreadPool.execute(() -> {
                            //if noone else has the chunk saved, the initiator peer sends another PUTCHUNK of the chunk
                            try {
                                this.peer.backupLostChunk(msg.fileID, Integer.parseInt(msg.chunkNO));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                }
            }
    }

    private void processDeleteChunk(Message msg) {
        this.peer.threadPool.execute(() -> {
            try {
                boolean ret = this.peer.deletechunks(msg.fileID); //deletes all chunks, return value is if successful or not
                if(ret && this.peer.protocolVersion.equals("1.1") && msg.version.equals("1.1")){
                    //additional response to the delete protocol
                    this.peer.sendPacket("DELETESUCESS", msg.fileID, null, null, "".getBytes());
                }
                if(this.peer.state.currentSize < this.peer.state.maxSize && this.peer.state.maxSize != -1 && this.peer.protocolVersion.equals("1.1")){
                    //if there is enough space available resumes listening to the thread
                    if(!this.peer.dataListener.connect)
                        this.peer.dataListener.startThread();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

    //responds to the GETCHUNK message
    private void processGetChunk(Message msg) {
        this.peer.getChunkMap.put(msg.fileID+"_"+msg.chunkNO,true);
        this.peer.threadPool.schedule(() -> {
            if(this.peer.getChunkMap.get(msg.fileID+"_"+msg.chunkNO) != null && this.peer.getChunkMap.get(msg.fileID+"_"+msg.chunkNO)) {
                try {
                    this.peer.getchunk(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, new Random().nextInt(400), TimeUnit.MILLISECONDS);

    }

    private void processPutChunk(String fileChunk,Message msg) {
            if (this.peer.state.filenameToFileID.get(msg.fileID) == null) {

                //saves the desired replication degree
                this.peer.state.desiredRepDegree.put(fileChunk, Integer.parseInt(msg.replicationDegree));

                //creates a set to save the peers that have the chunk saves
                this.peer.state.replicationDegreeMap.computeIfAbsent(fileChunk, k -> new HashSet<>());

                //introduces self into the set with the chunks
                this.peer.state.replicationDegreeMap.get(fileChunk).add(this.peer.peerID);

                this.peer.threadPool.schedule(() -> {
                    try {
                        if (this.peer.state.maxSize == -1 || this.peer.state.currentSize + (msg.body.length / 1000) < this.peer.state.maxSize) {
                            this.peer.putchunk(msg);
                            this.peer.restore.put(fileChunk, false);
                            this.peer.getChunkMap.put(msg.fileID + "_" + msg.chunkNO, false);

                        }
                        else if(this.peer.state.currentSize >= this.peer.state.maxSize && this.peer.state.maxSize != -1 && this.peer.protocolVersion.equals("1.1")){
                            //if no more space is available stops listening to the data channel
                            this.peer.dataListener.connect = false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, new Random().nextInt(400), TimeUnit.MILLISECONDS);
            }

    }
}
