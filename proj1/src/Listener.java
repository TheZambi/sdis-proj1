import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;
import java.util.List;

public class Listener {
    InetAddress group;
    Integer port;
    MulticastSocket socket;
    Peer peer;
    MessageInterpreter messageInterpreter;

    public Listener(String multicastInfo, Peer peer) throws Exception {
        this.peer = peer;
        this.messageInterpreter = new MessageInterpreter(peer);
        this.group = InetAddress.getByName(multicastInfo.split(":")[0]);
        this.port = Integer.parseInt(multicastInfo.split(":")[1]);
        this.socket = new MulticastSocket(this.port);
    }

    public void startThread() throws Exception {
        this.socket.joinGroup(this.group);
        this.peer.threadPool.execute(() -> {
            while(true){
                byte[] pack = new byte[64256];
                DatagramPacket recv = new DatagramPacket(pack, pack.length);
                try {
                    this.socket.receive(recv);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                byte[] withoutLeadingZeros = Arrays.copyOf(recv.getData(), recv.getLength());

                Message msg = new Message(withoutLeadingZeros);
                try {
                    this.messageInterpreter.interpretMessage(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }});
    }
}
