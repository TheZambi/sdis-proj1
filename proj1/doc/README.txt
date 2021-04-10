SDIS 20/21 PROJ1 REPORT T5G04

Compile:
	On src folder do: ../scripts/compile.sh

Setup:
	No setup needed

Run a peer:
	Innitiate the rmiregistry on the build folder.
	On src/build/ do: ../../scripts/peer.sh <protocol_version> <peer_id> <access_point> <multicast_control_address> <multicast_control_port> <multicast_data_backup_address> <multicast_data_backup_port> <multicast_data_recovery_address> <multicast_data_recovery_port>
	
	Where:
		protocol_version is 1.0 for base program, 1.1 for enhancements
		peer_id is an integer
		access_point is the rmi access point
		multicast_control_address and multicast_control_port are the address and the port for the multicast channel where control messages are sent
		multicast_data_backup_address and multicast_data_backup_port are the address and the port for the multicast channel where control messages are sent
		multicast_data_recovery_address and multicast_data_recovery_port are the address and the port for the multicast channel where control messages are sent

Send a command to a peer:
	On src/build/ do: ../../scripts/test.sh <peer_access_point> <command> {args}
	
	Where:
		peer_access_point is the rmi access point for the peer
		command is one of the following:
			STATE: no args needed, prints the peer's state (backed up chunks and files with respective details)
			BACKUP: backs up a file with desired replication degree. The args needed are the file and the replication degree in this order
			DELETE: deletes the chunks of a file from other peers. The args is only the file to be deleted
			RECLAIM: sets the maximum size a peer can use to store chunks. The args is the maximum size
			RESTORE: restores a backed up file. The args is the file to be restored.


Clean up:
	On src folder do: ../scripts/cleanup.sh <peer_id>
	Where:
		peer_id is the id of the peer to be cleaned up. The state is deleted as well as the chunk folder.