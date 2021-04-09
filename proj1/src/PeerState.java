import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PeerState implements Serializable {
    HashMap<String, Set<String>> replicationDegreeMap = new HashMap<>();
    HashMap<String, Integer> desiredRepDegree = new HashMap<>();
    HashMap<String, String> filenameToFileID = new HashMap<>();
    HashMap<String,Set<String>> deletedFilesFromPeers = new HashMap<>();
    Set<String> operations = new HashSet<>();
    long maxSize = -1;
    long currentSize = 0;
}
