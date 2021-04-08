import java.io.IOException;
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
    boolean connect;

    public Listener(String multicastInfo, Peer peer) throws Exception {
        this.peer = peer;
        this.messageInterpreter = new MessageInterpreter(peer);
        this.group = InetAddress.getByName(multicastInfo.split(":")[0]);
        this.port = Integer.parseInt(multicastInfo.split(":")[1]);
        this.socket = new MulticastSocket(this.port);
        this.connect = true;
    }

    public void startThread() throws Exception {
        this.socket.joinGroup(this.group);
        this.connect = true;
        this.peer.threadPool.execute(() -> {
            while(this.connect){
                byte[] pack = new byte[64256];
                DatagramPacket recv = new DatagramPacket(pack, pack.length);
                try {
                    this.socket.receive(recv);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                byte[] withoutLeadingZeros = Arrays.copyOf(recv.getData(), recv.getLength());

                Message msg = new Message(withoutLeadingZeros);
                msg.address = recv.getAddress();
                try {
                    this.messageInterpreter.interpretMessage(msg);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try {
                this.socket.leaveGroup(this.group);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}
