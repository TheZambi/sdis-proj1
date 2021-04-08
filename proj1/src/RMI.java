import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;

public interface RMI extends Remote {
    void backup(String filePath, Integer ReplicationDegree) throws Exception;
    void restore(String filePath) throws NoSuchAlgorithmException, IOException;
    void delete(String filePath) throws NoSuchAlgorithmException, IOException;
    String state() throws RemoteException;
    void reclaim(Integer maxSize) throws Exception;
}
