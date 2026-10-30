package comp512st.paxos;
import java.io.*;

final class PaxosMessage implements Serializable {
	// Type of messages:
	// phase1: PROPOSE -> PROMISE/REFUSE
	// pahse2: ACCEPT -> ACCEPTED/REJECT
	// phase3: CONFIRM
	enum Type {PROPOSE, PROMISE, REFUSE, ACCEPT, ACCEPTED, REJECT, CONFIRM};
	Type type;
	int slotIdx; // each slotIdx is one independent Paxos (each movement has one slotIdx)
	
	// initalize the ballot/ accpetedBallot/ promisedBallot with the Ballot.NOID
	// at PROMISE / ACCEPT message: proposer will sen their id
	// PROMISE / REFUSE / ACCEPTED / REJECT just bring the same id as the sender
	long ballot = Ballot.NOID; 
	long acceptedBallot = Ballot.NOID; // use in PROMISE
	long promisedBallot = Ballot.NOID; // when reject or refuse it will send back the largest id it have accpeted

	// PROMISE : value that acceptor accept before, ACCEPT : value that leader propose
	// CONFIRM: the final decision of the slot given by the leader
	Proposal value;// the value taken by different type of the message
	

	Proposal decision;

	private PaxosMessage(Type type, int slotIdx, long ballot) {
		this.type = type;
		this.slotIdx = slotIdx;
		this.ballot = ballot;
	}

	// Send by the proposer: Use the 'ballot_ID'  act as the leader of 'slot_ID'
	static PaxosMessage propose(long ballot, int slot) {
		return new PaxosMessage(Type.PROPOSE, slot, ballot);
	}

	// Promise reply from the acceptor and tell the proposer the value it accept before (for this slot)
	static PaxosMessage promise(int slot, long ballot, long acceptedBallot, Proposal val) {
		PaxosMessage pm = new PaxosMessage(Type.PROMISE, slot, ballot);
		pm.acceptedBallot = acceptedBallot; // it can be null;
		pm.value = val;
		return  pm;
	}

	// Refuse reply from the acceptor, beacuse the proposer's id is smaller than that from 
	// the previously promised one
	static PaxosMessage refuse(int slot, long ballot, long promisedBallot) {
		PaxosMessage pm = new PaxosMessage(Type.REFUSE, slot, ballot);
		pm.promisedBallot = promisedBallot;
		return pm;
	}

	// Send from the leader ask those acceptor to accept the value at the certain slot
	static PaxosMessage accept(int slot, long ballot, Proposal val) {
		PaxosMessage pm = new PaxosMessage(Type.ACCEPT, slot, ballot);
		pm.value = val;
		return pm;
	}

	// Reply from the acceptor, tell the leader they accpet the value
	static PaxosMessage accepted(int slot, long ballot)
	{
		return new PaxosMessage(Type.ACCEPTED, slot, ballot);
	}

	// Reject from the accptor, as it has accept the leader with greater BallotID
	static PaxosMessage reject(int slot, long ballot, long promisedBallot) {
		PaxosMessage pm = new PaxosMessage(Type.REJECT, slot, ballot);
		pm.promisedBallot = promisedBallot;
		return pm;

	}

	static PaxosMessage confirm(int slot, Proposal value) {
		PaxosMessage pm = new PaxosMessage(Type.CONFIRM, slot, Ballot.NOID);
		pm.value = value;
		return pm;
	}

	@Override 
	public String toString() {
		return type + " slot=" + slotIdx + " ballot=" + Ballot.str(ballot) + " value=" + value;
	}

}