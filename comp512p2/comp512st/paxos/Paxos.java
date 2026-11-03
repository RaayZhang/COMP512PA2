package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.logging.*;
import java.net.UnknownHostException;
import java.util.*;



// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{

	//---Thread Control---
	volatile boolean running= true; // other thread could been notify immediately
	Thread listnerThread; // listen to the GCL, read the message and handle them
	// --------------------

	// ---Proposer Parameters---
	static final long TIMEOUT = 1000; // wait for 1s after sending request
	// timeout might caused by process failure or network issue

	// After fail, it would wait for randomtime (10 - 100) and try again
	static final int BACKOFF_MAX = 100;
	//------------------------

	// ---Proposer Status---
	int maxRoundSeen = 0; // next time when it give a propose will use maxRoundSeen +1
	long localSequence = 0; // number of propose i sent before, use to generate proposal ID
	// Every propose will genrate a new Attempt
	// After it receive the promise, we can use it to check if the reply match with this attempt
	Attempt cur = null;
	//------------------------------

	final Random random = new Random();

	GCL gcl;
	FailCheck failCheck;
	Logger logger;

	String myProcess; // port of the process
	String[] otherProcesses;
	int id; // own process index in the index list
	int groupSize; // total number of the processes
	int majority; // grater than half of the groupSize

	// all fields must be accessed with the lock in your hand
	// prevent the case that one acceptor accept 2 different ballot at the same time
	final Object lock = new Object();

	// This will record the key:slot number -> value:what you accept for that slot
	// this only track my decision
	final Map<Integer, AcceptorSlot> acceptorStatus = new HashMap<>();


	// the next slot haven't decided yet
	// proposer will propose to act as the leader of this slot
	int nextToDecide = 1; // start from 1st slot

	// the next slot that could deliver to the application
	// every time we run the acceptTOMsg() it would += 1;
	int nextToDeliver = 1; // always <= nextToDecide

	// The slot that allowed to deliver should satisfy nextDeliver < nextToDecide
	// when nextDeliver == nextToDecide, need to wait for the next slot's decision

	// the decision made by the entire group, will not be modified
	// case1 (I am leader): received ACCEPTED from majority
	// case2 (I am voters): received CONFIRM from the leader
	// case3: other acceptor told be the slot has already been decided 
	final Map<Integer, Proposal> decided = new HashMap<>();

	// prevent same movement been run twice
	final Set<String> decidedIDs = new HashSet<>(); // wont have duplicate proposalID



	// logger is given by the TreasureIslandApp
	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		this.myProcess = myProcess;

		this.logger = logger;

		// Sort the member list so that every process will derives their unique id and same order
		// Use the TreeSet to sort the process also eliminate the duplicate process
		TreeSet<String> members = new TreeSet<>(Arrays.asList(allGroupProcesses));
		if (!members.contains(myProcess)) {
			members.add(myProcess);
		}
		List<String> sorted = new ArrayList<>(members);
		this.id = sorted.indexOf(myProcess);
		this.groupSize = sorted.size();
		this.majority = (this.groupSize / 2) + 1;
		List<String> others = new ArrayList<>(sorted);
		others.remove(myProcess);
		this.otherProcesses = others.toArray(new String[0]); // {} -> []

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

		// start the Listening Thread
		listnerThread = new Thread(() -> listenerLoop());
		listnerThread.setDaemon(true);
		listnerThread.start();


		// record the config of the group when there is a process start up, we can check the record inside\
		// <gameid>-<localhost>-<processinfo>-.log
		logger.info("Paxos: started " + myProcess + " id=" + id + " groupSize=" + groupSize
				+ " majority=" + majority + " others=" + Arrays.toString(otherProcesses));
		

	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{	Proposal p;
		synchronized (lock) {
			localSequence++;
			p = new Proposal(myProcess, localSequence, val, false);
		}
		logger.fine("Paxos: broadcastToMsg "+ p);

		while (running) {
			int slot;
			synchronized (lock) {
				// After receive confirm acceptor will add the id to the hashset
				// after proposer receive the reject, it might includes the decisonID
				// and the proposer record the id. 
				if (decidedIDs.contains(p.id)) return;
				slot = nextToDecide;
			}
			Proposal chosen = runSlot(slot, p);
			if (chosen == null) return;
			if (chosen.id.equals(p.id)) {
				logger.fine("Paxos: my move: " + p + " is in slot: " + slot);
				return;
			}
			
			// this slot has been occupied
			logger.fine("Paxos: slot " + slot + " went to " + chosen + ", retrying " + p);
		}

	}

	Proposal runSlot(int slot, Proposal p) {
		int attemptNum = 0;
		while (running) {
			attemptNum++;
			Attempt a;

			// Phase1: apply for the leader of the slot
			synchronized(lock) {
				// this slot been decided by others, dont need to apply for it
				if (decided.containsKey(slot)) return decided.get(slot);

				// choose a new round number which is grater than the max you ever seen
				maxRoundSeen++;
				// use it to generate a new ballot
				long ballot = Ballot.make(maxRoundSeen, id);

				// start a new attempt
				a = new Attempt(slot, ballot);
				cur = a;

				// proposer and acceptor can be the same process
				// vote for itself
				PaxosMessage vote = handlePropse(PaxosMessage.propose(ballot, slot));
				handleReply(myProcess, vote);

			}
			logger.fine("Paxos: PROPOSE slot=" + slot + " ballot=" + Ballot.str(a.ballot) + " attempt=" + attemptNum);
			sendToOthers(PaxosMessage.propose(a.ballot, slot));

			// fail point: After it send the request (for act as a leader)
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

			boolean isLeader;
			synchronized(lock) {
				waitForVotes(a);
				isLeader = a.votes() >= majority;
				
				// not leader and we lost this slot (been decided)
				if (!isLeader && decided.containsKey(slot)) {
					cur = null;
					return decided.get(slot);
				}
			
			}

			if (isLeader) {
				// fail point：多数派已经接受我为 leader 之后
				failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
				// Phase2: advertise one value
				Proposal value;
				synchronized (lock) {
					// if someone report that they have accepted some value
					// continue to ask them accept the value (with highest ballotID)
					if (a.highestAcceptedValue != null) {
						value = a.highestAcceptedValue;
					} else {
						value = p; // else use our own value
					}

					a.phase = 2; // move to phase2
					a.refused = false; // num_promised >= majority

					// accept the new value at it's own process
					PaxosMessage vote = handleAccept(PaxosMessage.accept(slot, a.ballot, value));
					handleReply(myProcess, vote);
				}
				logger.fine("Paxos: ACCEPT slot=" + slot + " ballot=" + Ballot.str(a.ballot) + " value=" + value);
				sendToOthers(PaxosMessage.accept(slot, a.ballot, value));

				boolean accepted;
				synchronized (lock) {
					waitForVotes(a);
					accepted = a.votes() >= majority;

					if (!accepted && decided.containsKey(slot)) {
						cur = null;
						return decided.get(slot);

					}
				}

				if (accepted) {
					// fail point：多数派已经接受了我提议的值之后，在通知别人之前。
					failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
					// most process have accpet it, so record the decision on it's own process
					// then notify everyone
					synchronized(lock) {
						record(slot, value);
						cur = null;
					}
					sendToOthers(PaxosMessage.confirm(slot, value));
					logger.fine("Paxos: DECIDED slot=" + slot + " value=" + value);
					return value;
				}
			}

			// this attempt fail, it's not the leader
			// wait for a while and try it again
			synchronized (lock) {
				cur = null ;
				long waitTime = 10 + random.nextInt(BACKOFF_MAX - 10 + 1);
				long deadline = System.currentTimeMillis() + waitTime;

				// wait until the deadline
				// if the slot been occupied during waiting,
				// the notifyAll() inside the record() will awake it
				while (running && !decided.containsKey(slot)) {
					long remaining = deadline - System.currentTimeMillis();
					if (remaining <= 0) break;
					try
					{
						lock.wait(remaining);
					}
					catch (InterruptedException ie)
					{
						Thread.currentThread().interrupt();   // 保留中断标志，然后放弃
						return null;
					}
				}

			}

		}
		return null;
	}

	// wait for the voting result, it would stop when one of the following conditions satisfied
	void waitForVotes(Attempt a) {
		long deadline = System.currentTimeMillis() + TIMEOUT;

		while (running && a.votes() < majority && !decided.containsKey(a.slot)) {
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				logger.fine("Paxos: timeout at phase: " + a.phase + " slot=" + a.slot + " ballot=" + Ballot.str(a.ballot));
				break;
			}
			try {
				lock.wait(remaining); // awaked by the notifyAll() inside handleReply / record
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		synchronized (lock) {
			while (true) {
				Proposal p = decided.get(nextToDeliver); // check is there a decision at nextDeliver slot
				if (p != null) {
					nextToDeliver++;
					lock.notifyAll(); // notify all threads wait for this decision
					if (p.noOperation) continue;  // No operation
					return p.val;
				}
				if (!running) throw new InterruptedException("Paxos is shut down");
				lock.wait(); // release the lock and wait, when it been awake it will check above conditions
			}
		}
	}



	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		// notify other threads
		synchronized (lock){
    		running = false;
    		lock.notifyAll();   // 叫醒所有在 lock 上 wait 的线程，让它们马上检查 running
		}
		// 2. 用一个新线程关闭 GCL，main 线程最多等 2 秒。
		//  正常情况下，shutdownGCL() 最后会往接收队列放一个关闭标记，
		// 监听线程的 readGCMessage() 读到它就会抛出 InterruptedException，自然退出。
		//  但如果别的玩家还开着而且不再发消息，shutdownGCL() 会卡住，所以不能无限期地等。
		Thread closer = new Thread(() -> gcl.shutdownGCL(), "paxos-gcl-shutdown");
		closer.setDaemon(true);
		closer.start();

		try {
			closer.join(2000); // 等 GCL 关闭，最多 2 秒
			listnerThread.join(500); // 再等监听线程退出，最多 0.5 秒
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		// 3. 后备：GCL 没能在时限内关好，监听线程还活着，就主动打断它。
		// 只有这种情况下才会出现那条 InterruptedException 的 SEVERE 日志。
		if (listnerThread.isAlive()) {
			logger.warning("Paxos: GCL did not finish shutting down in time, interrupting listener");
			listnerThread.interrupt();
		}
	}

	// get the status for the certain slot
	AcceptorSlot acceptor(int slot) {
		AcceptorSlot status = acceptorStatus.get(slot);
		if (status == null) {
			status = new AcceptorSlot();
			acceptorStatus.put(slot, status);
		}
		return  status;
	}

	// handle with the PROPOSE it received, return the refuse/promise message
	// Here it just make decision, the message will send by the other function
	PaxosMessage handlePropse(PaxosMessage pm) {
		synchronized(lock) {
			AcceptorSlot status = acceptor(pm.slotIdx);
			PaxosMessage reply;

			if (status.promisedBallot <  pm.ballot) {
				// when the propose ballot is larger than the ballot that i promised
				// i will promise to the new one, and return the value that i accept before
				status.promisedBallot = pm.ballot;
				reply = PaxosMessage.promise(pm.slotIdx, pm.ballot, status.acceptedBallot, status.valueAccepted);

			}
			else {
				// if the propose ballot is lower than that i promised -> refuse and send 
				// the ballot which i promised to the proposer
				reply = PaxosMessage.refuse(pm.slotIdx, pm.ballot, status.promisedBallot);
			}

			reply.decision = decided.get(pm.slotIdx); // if we already know the final decison of the slot, then tell proposer
			return reply;
		}
	}

	// handle with the ACCEPT (request from phase2). return the accepted/reject to the leader
	PaxosMessage handleAccept(PaxosMessage pm) {
		synchronized(lock) {
			AcceptorSlot status = acceptor(pm.slotIdx);
			PaxosMessage reply;

			if (pm.ballot >= status.promisedBallot) {
				// what if grater than that?
				status.promisedBallot = pm.ballot;  // accept -> promise
				status.acceptedBallot = pm.ballot;
				status.valueAccepted = pm.value;
				reply =  PaxosMessage.accepted(pm.slotIdx, pm.ballot);
			} else {
				// alredy promised the other proposer with grater ballot
				reply =  PaxosMessage.reject(pm.slotIdx, pm.ballot, status.promisedBallot); 
			}
			reply.decision = decided.get(pm.slotIdx);
			return reply;
		}
	}


	void handleReply(String senderAddress, PaxosMessage pm) {
		Attempt a = cur;
		if (a == null || a.slot != pm.slotIdx || a.ballot != pm.ballot) return;
		switch (pm.type) {
			case PROMISE:
				a.promises.add(senderAddress); // record who make promise\
				a.offerAccepted(pm.acceptedBallot, pm.value);
				break;
			case REFUSE:
				// only work in phase1, if we are at phase 2 the refuse it too late to stop it
				if (a.phase == 1) a.refused = true;
				break;
			case ACCEPTED:
				a.accepts.add(senderAddress);
				break;
			case REJECT:
				a.refused = true;
				break;
			default:
				break;
		}
		// awake the proposer that are waiting for votes waitForVotes()
		// let them compare the number of votes with majority
		lock.notifyAll();
	}

	// 对方reject时回复他之前最大的ballot编号，我们下次的编号要更大
	// 所以查看编号中的round然后更新maxRoundSeen, 调用方法需要有锁
	void updateBallot(long ballot)
	{
		int r = Ballot.round(ballot);
		if (r > maxRoundSeen) maxRoundSeen = r;
	}

	// ---------------------------------------------------------------------
	// Method use to record the final the decision of the certain slot
	void record(int slot, Proposal value) {
		Proposal p = decided.get(slot);
		if (p != null) {
			// the old proposal should equivalent to the new one
			if (!p.id.equals(value.id))
				logger.severe("Paxos: VIOLATION  at slot " + slot + " decided as " + p + " and " + value);
			return;
		}
		decided.put(slot, value); // record the decision
		decidedIDs.add(value.id); // this proposal has been decided

		while (decided.containsKey(nextToDecide)) {
			nextToDecide++;
		}
		logger.fine("Paxos: record slot " + slot + " = " + value);
		// we have new slot been decided sp notify all the thread that are waiting for the result
		lock.notifyAll();

	}


	// call the gcl.readGCMessage() to read from GCL and handle the message base on it's Msgtype
	void listenerLoop() {
		while (running) {
			GCMessage msg;
			try {
				msg = gcl.readGCMessage();
			} catch (InterruptedException e) {
				break;
			}
			if (!(msg.val instanceof PaxosMessage)) {
				logger.warning("Paxos: Unexpected message " + msg);
				continue;
			}
			// senderProcess is the sender's address
			handle(msg.senderProcess, (PaxosMessage) msg.val);
		}
	}

	void handle(String senderAddress, PaxosMessage pm) {
		switch (pm.type) {
			case PROPOSE:
				{
					// fail point: fail before it reply
					failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);
					PaxosMessage reply = handlePropse(pm); // <- 不在这里面send
					// send 不在 synchronized (lock) 里调用：onPropose 返回后就已经释放了锁，
					// 然后才发送。网络操作可能比较慢，持有锁去做会让其他线程（比如 acceptTOMsg）白白等待。

					send(reply, senderAddress); // send vote to the proposer
					// fail point：投票（PROMISE 或 REFUSE）发出之后立刻检查。
					failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
					break;
				}
			case ACCEPT:
				{
					PaxosMessage reply = handleAccept(pm); // accept / reject
					send(reply, senderAddress);
					break;
				}
			case CONFIRM:
				{
					synchronized(lock) {
						record(pm.slotIdx, pm.value);
					}
					break;
				}
			case PROMISE:
			case REFUSE:
			case ACCEPTED:
			case REJECT: {
				// when i am the proposer
				synchronized(lock) {
					// if the acceptor has final decision for that slot
					if (pm.decision != null) {
						record(pm.slotIdx, pm.decision);
					}
					// TODO 
					// when rejected, we need to use grater ballotId next time
					// update the maxRound we ever seen
					updateBallot(pm.promisedBallot);
					handleReply(senderAddress, pm);

				}
				break;
			}
			default: break;
		}
	}

	// ---------Sender method---------
	// point to point to one process
	void send(PaxosMessage pm, String destination) {
		try {
			gcl.sendMsg(pm, destination);
		} catch (Exception e) { // destination dead
			logger.warning("Paxos: failed to send " + pm + " to " + destination + ": " + e);
		}
	}

	// send to all other processes inside the group except itself
	void sendToOthers(PaxosMessage pm) {
		if (otherProcesses.length == 0) return;
		try {
			gcl.multicastMsg(pm, otherProcesses);
		} catch (Exception e) {
			logger.warning("Paxos: failed to multicast " + pm + ": " + e);
		}
	}



}

// this is a package scope helper class
// Ballot should statisfy:
// 1: ballot should be unique, 2 different proposers won't use the same id
// 2: acceptor can decide which propose they will accept/reject base on the ballot id
final class Ballot {
	// maximum number of process
	static final int MAX_PROCESS = 1000; // need to be grater than processID
	static final long NOID = -1L; // smaller than any real id

	// use the round and processID to get the ID
	static long make(int round, int processID) {
		return (long) round * MAX_PROCESS + processID;
	}

	// get the round number 
	static  int round(long ballot) {
		return  ballot < 0 ? 0: (int ) (ballot / MAX_PROCESS);
	}

	// take the processID
	static int process(long ballot) {
		return (int) (ballot % MAX_PROCESS);
	}

	// convert into the formal pattern: round.processID
	static String str(long ballot) {
		return ballot < 0 ? "NO_ID" : round(ballot) + "." + process(ballot);
	}

	private Ballot() {}

}


// The status at the certain slot, slot -> AcceptorSlot
final class AcceptorSlot {
	// the highest id it promised for this slot, initialize as (-1)
	long promisedBallot = Ballot.NOID;
	long  acceptedBallot = Ballot.NOID;
	Proposal valueAccepted = null;
}


