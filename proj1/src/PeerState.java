import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PeerState implements Serializable {
    //map with the ids of the peers with the saved chunks
    HashMap<String, Set<String>> replicationDegreeMap = new HashMap<>();
    //map with the desired replication degree for each chunk
    HashMap<String, Integer> desiredRepDegree = new HashMap<>();
    //converts the file ID to it's file name if backed up
    HashMap<String, String> filenameToFileID = new HashMap<>();
    //saves the ids of the peers that have not deleted the chunks
    HashMap<String,Set<String>> deletedFilesFromPeers = new HashMap<>();
    //set that saves the operations in progress
    Set<String> operations = new HashSet<>();
    long maxSize = -1;
    long currentSize = 0;
}
