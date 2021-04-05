import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class PeerState implements Serializable {
    HashMap<String, Set<String>> replicationDegreeMap = new HashMap<>();
    HashMap<String, Integer> desiredRepDegree = new HashMap<>();
    HashMap<String, String> filenameToFileID = new HashMap<>();
}
