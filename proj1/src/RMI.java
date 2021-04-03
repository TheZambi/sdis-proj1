import java.io.IOException;
import java.rmi.Remote;
import java.security.NoSuchAlgorithmException;

public interface RMI extends Remote {
    void backup(String filePath, Integer ReplicationDegree) throws Exception;
    void restore(String filePath) throws NoSuchAlgorithmException, IOException;

}
