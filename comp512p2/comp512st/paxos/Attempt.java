package comp512st.paxos;
import java.util.*;

class Attempt {
    // the slot and ballot used for this attempt
    final int slot;
    final long ballot;

    int phase = 1; // every attempt start with phase 1
    final Set<String> promises = new HashSet<>(); // record the address of the process that made the promise
    final Set<String> accepts = new HashSet<>(); // address set for phase2 (no duplicate)
    boolean refused = false; // when this attempt been refused or rejected, it should stop immediately

    Proposal highestAcceptedValue = null; // if this is not null at phase2, you should ask others to accept this value
    long highestAccptedBallot = Ballot.NOID;

    Attempt(int slot, long ballot) {
        this.slot = slot;
        this.ballot = ballot;
    }

    // how many votes it received at current phase
    int votes()
	{
		if (phase == 1) return promises.size();
		else return accepts.size();
	}

    // 收到一个 PROMISE 里报告的已接受值：如果比目前记录的编号更大，就替换。
	void offerAccepted(long accBallot, Proposal accValue)
	{
		if (accValue != null && accBallot > highestAccptedBallot)
		{
			highestAccptedBallot = accBallot;
			highestAcceptedValue = accValue;
		}
	}


}
