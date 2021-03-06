import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Message {
    InetAddress address = null;
    String version;
    String messageType;
    String peerID;
    String fileID;
    String chunkNO;
    String replicationDegree;
    byte[] body;

    public Message(byte[] msg) {
        String s = new String(msg); //ignores following \0
        String[] arr = s.split("\r\n\r\n"); //separates body from header
        Integer index = s.indexOf("\r\n\r\n") + 4;
        String header = arr[0];
        byte[] body;
        if(arr.length >=2)
            body = Arrays.copyOfRange(msg, index, msg.length); //copies from the original array to maintain encoding
        else
            body = new byte[]{}; //if there is not body, creates an empty body so operations can be performed on it

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
