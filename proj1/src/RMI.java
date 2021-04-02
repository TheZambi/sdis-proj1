import java.rmi.Remote;

public interface RMI extends Remote {
    void backup(String filePath, Integer ReplicationDegree) throws Exception;

}
