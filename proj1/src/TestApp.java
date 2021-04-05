import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp {
    private TestApp() {
    }

    public static void main(String[] args) {
        try {
            Registry registry = LocateRegistry.getRegistry();
            RMI stub = (RMI) registry.lookup(args[0]);
            switch (args[1]) {
                case "BACKUP":
                    stub.backup(args[2], Integer.parseInt(args[3]));
                    break;
                case "RESTORE":
                    stub.restore(args[2]);
                    break;
                case "DELETE":
                    stub.delete(args[2]);
                    break;
                case "STATE":
                    stub.state();
                    break;
                default:
                    System.out.println("No method found for: " + args[1]);
            }

        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
