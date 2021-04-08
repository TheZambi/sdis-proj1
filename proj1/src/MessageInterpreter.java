import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;
import static java.lang.System.setOut;

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

            }
        }
    }

    private void processChunk(Message msg) throws IOException, NoSuchAlgorithmException {
        this.peer.getChunkMap.put(msg.fileID+"_"+msg.chunkNO,false);
        int chunkNO = Integer.parseInt(msg.chunkNO) + 1;
        if (this.peer.restoreFile.get(msg.fileID) != null) {
            byte[] body = null;

            if(this.peer.protocolVersion.equals("1.0") || msg.version.equals("1.0")) {
                body = msg.body;
            }
            else if(this.peer.protocolVersion.equals("1.1") && msg.version.equals("1.1")) {
                int nTries = 0;
                Socket socket = null;
                while(nTries<5) {
                    try {
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
                int bytesRead = 0, totalBytesRead = 0;
                while((bytesRead = in.read(buf))!=-1)
                {
                    arraycopy(buf, 0, aux, totalBytesRead, bytesRead);
                    totalBytesRead+=bytesRead;
                }
                body = new byte[totalBytesRead];
                arraycopy(aux,0,body,0,totalBytesRead);
                in.close();
                socket.close();
            }

            this.peer.saveChunk(msg.fileID, msg.chunkNO, body);
            if (body.length != 64000) {
                this.peer.restoreFile.put(msg.fileID, false);
                this.peer.saveFile(msg.fileID);
                this.peer.state.operations.remove("restore-" + msg.fileID);
                this.peer.restoreFile.remove(msg.fileID);
            }
            else if (this.peer.restoreFile.get(msg.fileID)) {
                this.peer.restore(chunkNO,msg.fileID);
            }
        }
    }

    private void processRemoveChunk(String fileChunk, Message msg) throws Exception {
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
                this.peer.deletechunks(msg.fileID);
                if(this.peer.state.currentSize < this.peer.state.maxSize && this.peer.state.maxSize != -1 && this.peer.protocolVersion.equals("1.1")){
                    if(!this.peer.dataListener.connect)
                        this.peer.dataListener.startThread();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }

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

                this.peer.state.desiredRepDegree.put(fileChunk, Integer.parseInt(msg.replicationDegree));

                this.peer.state.replicationDegreeMap.computeIfAbsent(fileChunk, k -> new HashSet<>());

                this.peer.state.replicationDegreeMap.get(fileChunk).add(this.peer.peerID);

                this.peer.threadPool.schedule(() -> {
                    try {
                        if (this.peer.state.maxSize == -1 || this.peer.state.currentSize + (msg.body.length / 1000) < this.peer.state.maxSize) {
                            this.peer.putchunk(msg);
                            this.peer.restore.put(fileChunk, false);
                            this.peer.getChunkMap.put(msg.fileID + "_" + msg.chunkNO, false);

                        }
                        else if(this.peer.state.currentSize >= this.peer.state.maxSize && this.peer.state.maxSize != -1 && this.peer.protocolVersion.equals("1.1")){
                            this.peer.dataListener.connect = false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, new Random().nextInt(400), TimeUnit.MILLISECONDS);
            }

    }
}
