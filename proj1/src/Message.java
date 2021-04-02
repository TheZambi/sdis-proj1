import java.net.DatagramPacket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Message {
    String version;
    String messageType;
    String peerID;
    String fileID;
    String chunkNO;
    String replicationDegree;
    byte[] body;

    public Message(byte[] msg) {
        String s = new String(msg, StandardCharsets.UTF_8); //ignores following \0
        String[] arr = s.split("\r\n\r\n"); //separates body from header
        String header = arr[0];
        byte[] body=null;
        if(arr.length ==2)
            body = arr[1].getBytes();
        String[] aux = header.split(" ",4);
        this.version = aux[0];
        this.messageType = aux[1];
        this.peerID = aux[2];

        String fileID = "";
        for(int i=0; i<64; i++)
            fileID += aux[3].charAt(i); //gets the next 64 bytes which correspond to the encrypted file id

        this.fileID = fileID;
        String[] left = aux[3].substring(65).split(" "); //gets the arguments that might come after
        for(int i=0; i<left.length;i++)
        {
            if(i==0)
                this.chunkNO = left[0];
            if(i==1)
                this.replicationDegree = left[1];
        }

        this.body = body;
    }
}
