import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp {
    private TestApp() {
    }

    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.getRegistry();
            RMI stub = (RMI) registry.lookup(args[0]);
            if (args[1].equals("BACKUP")) {
                stub.backup(args[2], Integer.parseInt(args[3]));
            }
            else if(args[1].equals("RESTORE")){
                stub.restore(args[2]);
            }
            else if(args[1].equals("DELETE")){
                stub.delete(args[2]);
            }
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
