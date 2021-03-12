import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class Server {

    private static DatagramSocket dataSocket = null;

    void server(String[] args) throws IOException {
        HashMap<String,String> map = new HashMap<>();

        int port = Integer.parseInt(args[0]);

        dataSocket = new DatagramSocket(port);

        Timer timer = new Timer(true);

        ServerAnnouncement sa = new ServerAnnouncement(args);
        timer.scheduleAtFixedRate(sa,0,1000);

        while(true) {
            byte[] buf = new byte[256];
            DatagramPacket dataPack = new DatagramPacket(buf,buf.length);

            dataSocket.receive(dataPack);
            String s = new String(dataPack.getData());

            String[] arr = s.split("[ |\u0000]");

            if(s.contains("register")) {
                map.put(arr[1], arr[2]);
                String reply = "success";
                dataSocket.send(new DatagramPacket(reply.getBytes(), reply.getBytes().length, dataPack.getAddress(), dataPack.getPort()));
            }
            else if(s.contains("lookup"))
            {
                String reply = map.get(arr[1]);
                dataSocket.send(new DatagramPacket(reply.getBytes(), reply.getBytes().length, dataPack.getAddress(), dataPack.getPort()));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        new Server().server(args);
    }


}
