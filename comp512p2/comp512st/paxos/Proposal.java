package comp512st.paxos;
import java.io.*;
import java.util.*;

// the accpeted value in Paxos -> final decision
final class Proposal implements Serializable {
	// sequence number will increase so each process will have different id
	final String id; // we can use the id to determine if the proposal is the same

	final Object val; // the value we need to broadcast
	// in this game the value is Object[] {id, direction_character}

	// when the proposer crash before it send the CONFIRM a slot get stuck
	// other process will rerun the Paxos and recover the decision of this process
	// if the slot ends up with a decision: NO_OPERATION then they will skip it
	final boolean noOperation; 

	Proposal(String source, long seqNum, Object val, boolean noOperation) {
		this.id = source + "#" + seqNum; // combination of the src process and seqNum
		this.val = val;
		this.noOperation = noOperation;
	}

	// 2 type of values will be recorded:
	// 1: [id, val](uniqueID)  2: noOperation(uniqueID)
	@Override 
	public String toString() {
		if (noOperation) return "NO_OPERATION(" + id + ")";
		else {
			String value = (val instanceof Object[]) ? Arrays.toString((Object[]) val) : String.valueOf(val);
			return value + "(" + id + ")";
		}
	}
	

}

