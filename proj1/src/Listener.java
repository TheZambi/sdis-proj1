import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;

public class Listener {
    InetAddress group;
    Integer port;
    MulticastSocket socket;
    Peer peer;

    public Listener(String multicastControl, Peer peer) throws Exception {
        this.peer = peer;
        this.group = InetAddress.getByName(multicastControl.split(":")[0]);
        this.port = Integer.parseInt(multicastControl.split(":")[1]);
        this.socket = new MulticastSocket(this.port);
    }

    public void startThread() throws Exception {
        this.socket.joinGroup(this.group);
        Thread listenerThread = new Thread(() -> {
            while(true){
                byte[] pack = new byte[64256];
                DatagramPacket recv = new DatagramPacket(pack, pack.length);
                try {
                    this.socket.receive(recv);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                List<String> msg = this.peer.parseMessage(recv);
                try {
                    this.peer.interpretMessage(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }});
        listenerThread.start();
    }
}
