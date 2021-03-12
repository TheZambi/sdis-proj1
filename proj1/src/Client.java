import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Client {

    public static void main(String[] args) throws IOException, InterruptedException {
        int multPort = Integer.parseInt(args[1]);
        String multiAddr = args[0];
        String oper = args[2];
        String myRequest;

        if(oper.equals("register"))
        {
            String myDNS = args[3];
            String myIP = args[4];
            myRequest = oper + " " + myDNS + " " + myIP;
        }
        else if(oper.equals("lookup"))
        {
            String myDNS = args[3];
            myRequest = oper + " " + myDNS;
        }
        else
        {
            return;
        }

        InetAddress group = InetAddress.getByName(multiAddr);

        MulticastSocket multiSocket = new MulticastSocket(multPort);
        multiSocket.joinGroup(group);


        byte[] buf2 = new byte[256];
        DatagramPacket serverBroadcast = new DatagramPacket(buf2,buf2.length);

        multiSocket.receive(serverBroadcast);
        String response = new String(serverBroadcast.getData(), StandardCharsets.UTF_8);


        String[] arr = response.split("[-|\u0000]");

        byte[] buf = new byte[256];
        DatagramPacket reply = new DatagramPacket(buf,buf.length);

        multiSocket.receive(reply);
        System.out.println("Received server info: " + new String(reply.getData()));
        multiSocket.leaveGroup(group);
        multiSocket.close();

        DatagramSocket dataSocket = new DatagramSocket();

        DatagramPacket datagramPacket = new DatagramPacket(myRequest.getBytes(), myRequest.getBytes().length, InetAddress.getByName(arr[0]), Integer.parseInt(arr[1]));
        dataSocket.send(datagramPacket);

        buf = new byte[256];
        DatagramPacket reply2 = new DatagramPacket(buf,buf.length);
        dataSocket.receive(reply2);

        String s = new String(reply2.getData(), StandardCharsets.UTF_8);
        System.out.println("Client: " + s);

    }
}
