import java.io.IOException;
import java.net.*;
import java.sql.CallableStatement;
import java.util.TimerTask;

public class ServerAnnouncement extends TimerTask {

    private MulticastSocket multicastSocket = null;
    private DatagramPacket datagramPacket = null;
    int port = 0;

    public ServerAnnouncement(String[] args) throws IOException {
        port = Integer.parseInt(args[0]);
        String multiAddr = args[1];
        int multPort = Integer.parseInt(args[2]);


        InetAddress group = InetAddress.getByName(multiAddr);

        multicastSocket = new MulticastSocket(multPort);
        multicastSocket.joinGroup(group);

        String myRequest = "localhost-"+port;

        datagramPacket = new DatagramPacket(myRequest.getBytes(), myRequest.getBytes().length, group, multPort);

    }



    public void close()
    {
        multicastSocket.close();
    }

    @Override
    public void run() {
        System.out.println("sending announcement");
        try {
            multicastSocket.send(datagramPacket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
