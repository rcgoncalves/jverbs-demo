package net.rcgoncalves.jverbs.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import com.ibm.net.rdma.jverbs.cm.ConnectionEvent;
import com.ibm.net.rdma.jverbs.cm.ConnectionId;
import com.ibm.net.rdma.jverbs.cm.ConnectionParameter;
import com.ibm.net.rdma.jverbs.cm.EventChannel;
import com.ibm.net.rdma.jverbs.cm.PortSpace;
import com.ibm.net.rdma.jverbs.verbs.CompletionChannel;
import com.ibm.net.rdma.jverbs.verbs.CompletionQueue;
import com.ibm.net.rdma.jverbs.verbs.MemoryRegion;
import com.ibm.net.rdma.jverbs.verbs.PollCQMethod;
import com.ibm.net.rdma.jverbs.verbs.PostReceiveMethod;
import com.ibm.net.rdma.jverbs.verbs.PostSendMethod;
import com.ibm.net.rdma.jverbs.verbs.ProtectionDomain;
import com.ibm.net.rdma.jverbs.verbs.QueuePair;
import com.ibm.net.rdma.jverbs.verbs.QueuePairInitAttribute;
import com.ibm.net.rdma.jverbs.verbs.QueuePairLimit;
import com.ibm.net.rdma.jverbs.verbs.ReceiveWorkRequest;
import com.ibm.net.rdma.jverbs.verbs.RegisterMemoryRegionMethod;
import com.ibm.net.rdma.jverbs.verbs.RequestNotifyCQMethod;
import com.ibm.net.rdma.jverbs.verbs.ScatterGatherElement;
import com.ibm.net.rdma.jverbs.verbs.SendWorkRequest;
import com.ibm.net.rdma.jverbs.verbs.VerbsContext;
import com.ibm.net.rdma.jverbs.verbs.WorkCompletion;

/**
 * Simple network client implemented using jVerbs.
 * The client connects to a server, and sends a configurable number of messages, waiting for a
 * reply from the server after each message sent. 
 * The messages the client sends contain a random integer, and another integer that was part of the
 * last message received.  In this way, whenever a message is received, the client can validate it
 * by checking if it contains the last random integer it sent.
 * 
 * @author Rui Carlos Gonçalves <rgoncalves.pt@gmail.com>
 */
public class Client {	
	/**
	 * Runs the client.
	 * The IP and port of the server can be provided as arguments.
	 * By default, it tries to connect to localhost on port 54321.
	 * 
	 * @param args optional hostname and port of the server.
	 */
	public static void main(String[] args) {
		String hostname = args.length >= 1 ? args[0] : "127.0.0.1";
		int port = args.length >= 2 ? Integer.parseInt(args[1]) : 54321;
		Client client = null;
		
		try {
			client = new Client(new InetSocketAddress(hostname, port));
			client.connect();
			client.run();
		}
		catch(Exception e) {
			client.setFailed();
			e.printStackTrace();
		}
		finally {
			if(client != null) {
				client.disconnect();
			}
		}
	}
	
	// =============================================================================================
	/** Channel for tracking connection events. */
	private final EventChannel events;
	/** The connection with the server. */
	private final ConnectionId client;
	
	/** The verbs context of the device. */
	private VerbsContext context;
	/** The protection domain of the device. */
	private ProtectionDomain protectionDomain;
	/** Channel for tracking work completion events. */
	private CompletionChannel completionChannel;
	/** Queue for work completions. */
	private CompletionQueue completionQueue;
	/** Stateful method to request work completion events notifications. */
	private RequestNotifyCQMethod requestNCQ;
	/** Stateful method to poll queue for work completions. */
	private PollCQMethod pollCQ;
	/** Queue pair to post work requests. */
	private QueuePair queuePair;
	/** Memory region for the buffers. */
	private MemoryRegion memoryRegion;
	/** Buffer to receive messages. */
	private ByteBuffer recvBuffer;
	/** Buffer to send messages. */
	private ByteBuffer sendBuffer;
	/** Stateful method to post the receive work request. */
	private PostReceiveMethod postRWR;
	/** Stateful method to post the send work request. */
	private PostSendMethod postSWR;
 	
	/** Specifies whether there was a failure. */
	private boolean failed;
	/** Previous local message sent (or 0, if no message was sent yet). */
	private int message;
	
	/**
	 * Creates a client object, and resolves the server address and route.
	 * 
	 * @param addr the server socket address.
	 * @throws IOException 
	 */
	public Client(InetSocketAddress addr) throws IOException {
		this.message = 0;
		this.failed = false;
		this.events = EventChannel.createEventChannel();
		this.client = ConnectionId.create(this.events, PortSpace.RDMA_PS_TCP);
		this.client.resolveAddress(null, addr, 1000);
		ConnectionEvent event = this.events.getConnectionEvent(-1);
		if(event.getEventType() != ConnectionEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED) {
			this.events.ackConnectionEvent(event);
			throw new IOException("resolve address failed");
		}
		this.events.ackConnectionEvent(event);
		this.client.resolveRoute(1000);
		event = this.events.getConnectionEvent(-1);
		if(event.getEventType() != ConnectionEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED) {
			this.events.ackConnectionEvent(event);
			throw new IOException("resolve route failed");
		}
		this.events.ackConnectionEvent(event);
	}
	
	/**
	 * Sets the failed state of this client.
	 */
	public void setFailed() {
		this.failed = true;
	}
	
	/**
	 * Initializes the verbs data structures, and starts a connection with the server.
	 * 
	 * @throws IOException if an I/O error occurs.
	 */
	public void connect() throws IOException {
		ConnectionEvent event;
		// create auxiliary data structures
		this.context = this.client.getVerbsContext();
		this.protectionDomain = this.context.allocProtectionDomain();
		this.completionChannel = this.context.createCompletionChannel();
		this.completionQueue = this.context.createCompletionQueue(this.completionChannel, 100, 0);
		this.requestNCQ = this.completionQueue.requestNotifyCQ(false);
		if(!this.requestNCQ.execute().isSuccess()) {
			throw new IOException("request notify failed");
		}
		this.queuePair = this.client.createQueuePair(this.protectionDomain,
				new QueuePairInitAttribute(
						new QueuePairLimit(),
						QueuePair.Type.IBV_QPT_RC,
						this.completionQueue,
						this.completionQueue)
		);
		// prepare memory buffers
		ByteBuffer buffer = ByteBuffer.allocateDirect(2 * Server.MESSAGE_SIZE);
		buffer.position(0).limit(Server.MESSAGE_SIZE);
		this.sendBuffer = buffer.slice();
		buffer.position(Server.MESSAGE_SIZE).limit(2 * Server.MESSAGE_SIZE);
		this.recvBuffer = buffer.slice();
		RegisterMemoryRegionMethod registerMR = this.protectionDomain.registerMemoryRegion(
				buffer, MemoryRegion.IBV_ACCESS_LOCAL_WRITE);
		if(!registerMR.execute().isSuccess()) {
			registerMR.free();
			throw new IOException("register memory failed");
		}
		this.memoryRegion = registerMR.getMemoryRegion();
		registerMR.free();
		// prepare receive work request
		ScatterGatherElement sge = new ScatterGatherElement();
		sge.setAddress(this.memoryRegion.getAddress() + Server.MESSAGE_SIZE);
		sge.setLength(Server.MESSAGE_SIZE);
		sge.setLocalKey(this.memoryRegion.getLocalKey());
		ReceiveWorkRequest recvWR = new ReceiveWorkRequest();
		recvWR.setWorkRequestId(0);
		recvWR.setSgeList(new LinkedList<ScatterGatherElement>(Arrays.asList(sge)));
		// post receive work request
		this.postRWR = queuePair.preparePostReceive(Collections.singletonList(recvWR));
		if(!this.postRWR.execute().isSuccess()) {
			throw new IOException("post receive failed");
		}
		// accept connection
		this.client.connect(new ConnectionParameter());
		event = this.events.getConnectionEvent(-1);
		if(event.getEventType() != ConnectionEvent.EventType.RDMA_CM_EVENT_ESTABLISHED) {
			this.events.ackConnectionEvent(event);
			throw new IOException("accept connection failed");
		}
		this.events.ackConnectionEvent(event);
		// prepare send work request
		sge = new ScatterGatherElement();
		sge.setAddress(this.memoryRegion.getAddress());
		sge.setLength(Server.MESSAGE_SIZE);
		sge.setLocalKey(this.memoryRegion.getLocalKey());
		SendWorkRequest sendWR = new SendWorkRequest();
		sendWR.setWorkRequestId(0);
		sendWR.setOpcode(SendWorkRequest.Opcode.IBV_WR_SEND);
		sendWR.setSendFlags(SendWorkRequest.IBV_SEND_SIGNALED);
		sendWR.setSgeList(new LinkedList<ScatterGatherElement>(Arrays.asList(sge)));
		// prepare post send work request
		this.postSWR = this.queuePair.preparePostSend(Collections.singletonList(sendWR));
	}
	
	/**
	 * Processes work requests.
	 * Sends N_MESSAGES messages to the server, and waits for a reply after each message.
	 * The messages the client sends contain a random integer, and another integer that was part of
	 * the last message received.  In this way, whenever a message is received, the client can
	 * validate it by checking if it contains the last random integer it sent.
	 * Before sending the reply, the receive work request is reposted.
	 * 
	 * @throws IOException if an I/O error occurs, or if the message received does not
	 * meet the requirements.
	 */
	public void run() throws IOException {
		WorkCompletion wc = new WorkCompletion();
		this.pollCQ = this.completionQueue.pollCQ(new WorkCompletion[] { wc }, 1);
		int localMessage, remoteMessage = 0;
		// prepare send work request
		this.sendBuffer.clear();
		this.message = (int) (Math.random() * Integer.MAX_VALUE);
		this.sendBuffer.putInt(this.message).putInt(remoteMessage);
		// post first send work request
		if(!this.postSWR.execute().isSuccess()) {
			throw new IOException("post send work request failed");
		}
		// posted work requests waiting for completion
		int posted = 2;
		// received messages
		int count = 0;
		while(posted > 0) {
			// wait for work completions
			if(this.completionChannel.getCQEvent(-1)) {
				this.completionChannel.ackCQEvents(this.completionQueue, 1);
				if(!this.requestNCQ.execute().isSuccess()) {
					throw new IOException("request notification failed");
				}
			}
			// poll all available work completions
			while(this.pollCQ.execute().isSuccess() && this.pollCQ.getPolls() > 0) {
				posted--;
				if(wc.getOpcode() == WorkCompletion.Opcode.IBV_WC_SEND) {
					if(wc.getStatus() != WorkCompletion.Status.IBV_WC_SUCCESS) {
						throw new IOException("send work request failed");
					}
				}
				else if(wc.getOpcode() == WorkCompletion.Opcode.IBV_WC_RECV) {
					if(wc.getStatus() != WorkCompletion.Status.IBV_WC_SUCCESS) {
						throw new IOException("receive work request failed");
					}
					else {
						count++;
						// read received data
						this.recvBuffer.clear();
						remoteMessage = this.recvBuffer.getInt();
						localMessage = this.recvBuffer.getInt();
						// check if the message we sent was preserved
						if(localMessage != this.message) {
							throw new IOException("invalid message received");
						}
						// are there more messages to send/receive?
						if(count < Server.N_MESSAGES) {
							// repost receive work request
							if(!this.postRWR.execute().isSuccess()) {
								throw new IOException("post receive work request failed");
							}
							posted++;
							// prepare send work request
							this.sendBuffer.clear();
							this.message = (int) (Math.random() * Integer.MAX_VALUE);
							this.sendBuffer.putInt(this.message).putInt(remoteMessage);
							// repost send work request
							if(!this.postSWR.execute().isSuccess()) {
								throw new IOException("post send work request failed");
							}
							posted++;
						}
					}
				}
			}
		}
	}
	
	/**
	 * Waits for disconnect, and destroys the allocated data structures.
	 * In case of error, it continues trying to destroy remaining data structures.
	 */
	public void disconnect() {
		// disconnect
		try {
			this.client.disconnect();
			if(!this.failed) {
				ConnectionEvent event = this.events.getConnectionEvent(-1);
				if(event.getEventType() != ConnectionEvent.EventType.RDMA_CM_EVENT_DISCONNECTED) {
					this.events.ackConnectionEvent(event);
					throw new IOException("unexpected event");
				}
				this.events.ackConnectionEvent(event);
			}
		}
		catch(IOException e) { e.printStackTrace(); }
		// destroy stateful methods
		if(this.requestNCQ != null) {
			this.requestNCQ.free();
		}
		if(this.pollCQ != null) {
			this.pollCQ.free();
		}
		if(this.postSWR != null) {
			this.postSWR.free();
		}
		if(this.postRWR != null) {
			this.postRWR.free();
		}
		// destroy memory regions
		if(this.memoryRegion != null) {
			try {
				this.protectionDomain.deregisterMemoryRegion(this.memoryRegion).execute().free();
			}
			catch(Exception e) { e.printStackTrace(); }
		}
		// destroy queue pair and client connection
		if(this.client != null) {
			try {
				this.client.destroyQueuePair();
				this.client.destroy();
			}
			catch(Exception e) { e.printStackTrace(); }
		}
		// destroy completion queue, completion channel, and protection domain
		if(this.context != null) {
			try {
				if(this.completionQueue != null) {
					this.context.destroyCompletionQueue(this.completionQueue);
				}
				if(this.completionChannel != null) {
					this.context.destroyCompletionChannel(this.completionChannel);
				}
				if(this.protectionDomain != null) {
					this.context.deallocProtectionDomain(this.protectionDomain);
				}
			}
			catch(Exception e) { e.printStackTrace(); }
		}
		// destroy server and events channel
		try {
			this.events.destroyEventChannel();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
}