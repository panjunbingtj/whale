package org.apache.storm.messaging.rdma;

import com.ibm.disni.channel.*;
import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/9/11.
 */
public class Client extends ConnectionWithStatus implements IStatefulObject {
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionWithStatus.class);

    private static final String PREFIX = "RDMA-Client-";
    private static final Timer timer = new Timer("RDMA-ChannelAlive-Timer", true);
    private static final long NO_DELAY_MS = 0L;

    private final StormBoundedExponentialBackoffRetry retryPolicy;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<RdmaChannel> channelRef = new AtomicReference<>();

    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

    /**
     * Total number of connection attempts.
     */
    private final AtomicInteger totalConnectionAttempts = new AtomicInteger(0);

    /**
     * Number of connection attempts since the last disconnect.
     */
    private final AtomicInteger connectionAttempts = new AtomicInteger(0);

    /**
     * Number of messages successfully sent to the remote destination.
     */
    private final AtomicInteger messagesSent = new AtomicInteger(0);

    /**
     * Number of messages that could not be sent to the remote destination.
     */
    private final AtomicInteger messagesLost = new AtomicInteger(0);

    /**
     * Periodically checks for connected channel in order to avoid loss
     * of messages
     */
    private final long CHANNEL_ALIVE_INTERVAL_MS = 30000L;

    /**
     * Number of messages buffered in memory.
     */
    private final AtomicLong pendingMessages = new AtomicLong(0);

    private final InetSocketAddress dstAddress;

    private final String dstHost;
    protected final String dstAddressPrefixedName;
    private final Context context;

    private final Object writeLock = new Object();
    private final Map<String, Object> topoConf;
    private RdmaNode rdmaClient;
    private RdmaChannel rdmaChannel;

    private final MessageBuffer batcher;

    ///RDMA HOSTNAME
    private final String IBAddress;
    private HashedWheelTimer scheduler;
    //////////////////////////////////////RDMA///////////////////////////////////

    Client(Map<String, Object> topoConf, String host, int port, Context context) throws Exception {
        this.topoConf = topoConf;
        this.context = context;
        closing = false;

        LOG.info("creating Netty Client, connecting to {}:{}", host, port);
        dstHost = host;
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);

        ///add IBAddress Configure
        BufferedReader bufferedReader=new BufferedReader(new FileReader("/whale/RDMAHostName"));
        this.IBAddress=bufferedReader.readLine();

        scheduler = new HashedWheelTimer(new NettyRenameThreadFactory("client-schedule-service"));

        int messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);

        int maxReconnectionAttempts = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        batcher = new MessageBuffer(messageBatchSize);

        this.rdmaClient=new RdmaNode(IBAddress, true, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {

            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        }, (remote,rdmaChannel) -> {

        });

        launchChannelAliveThread();
        scheduleConnect(NO_DELAY_MS);
    }

    /**
     * This thread helps us to check for channel connection periodically.
     * This is performed just to know whether the destination address
     * is alive or attempts to refresh connections if not alive. This
     * solution is better than what we have now in case of a bad channel.
     */
    private void launchChannelAliveThread() {
        // netty TimerTask is already defined and hence a fully
        // qualified name
        timer.schedule(new java.util.TimerTask() {
            public void run() {
                try {
                    LOG.debug("running timer task, address {}", dstAddress);
                    if(closing) {
                        this.cancel();
                        return;
                    }
                    getConnectedChannel();
                } catch (Exception exp) {
                    LOG.error("channel connection error {}", exp);
                }
            }
        }, 0, CHANNEL_ALIVE_INTERVAL_MS);
    }

    private RdmaChannel getConnectedChannel() {
        RdmaChannel channel = channelRef.get();
        if (connectionEstablished(channel)) {
            return channel;
        } else {
            // Closing the channel and reconnecting should be done before handling the messages.
            boolean reconnectScheduled = false;
            try {
                reconnectScheduled = closeChannelAndReconnect(channel);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (reconnectScheduled) {
                // Log the connection error only once
                LOG.error("connection to {} is unavailable", dstAddressPrefixedName);
            }
            return null;
        }
    }

    /**
     * Schedule a reconnect if we closed a non-null channel, and acquired the right to
     * provide a replacement by successfully setting a null to the channel field
     * @param channel
     * @return if the call scheduled a re-connect task
     */
    private boolean closeChannelAndReconnect(RdmaChannel channel) throws IOException, InterruptedException {
        if (channel != null) {
            channel.stop();
            if (channelRef.compareAndSet(channel, null)) {
                scheduleConnect(NO_DELAY_MS);
                return true;
            }
        }
        return false;
    }

    private boolean connectionEstablished(RdmaChannel channel) {
        return channel != null && channel.isConnected();
    }

    private void scheduleConnect(long delayMs) {
        scheduler.newTimeout(new Connect(dstAddress), delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        } else {
            if (rdmaChannel.isConnected()) {
                return Status.Ready;
            } else {
                return Status.Connecting; // need to wait until sasl channel is also ready
            }
        }
    }

    @Override
    public void registerRecv(IConnectionCallback cb) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        throw new RuntimeException("Client connection should not send load metrics");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        WorkerMessage workerMessage=new WorkerMessage(Arrays.asList(taskId),payload);
        send(workerMessage);
    }

    ////////////////////////////////////优化transferAllGrouping/////////////////////////
    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public void send(WorkerMessage msgs) {
        LOG.debug("Client send msg : {}",msgs);
        if (closing) {
            int numMessages = msgs.tasks().size();
            LOG.error("discarding {} messages because the Netty client to {} is being closed", numMessages,
                    dstAddressPrefixedName);
            return;
        }

        if (!hasMessages(msgs)) {
            return;
        }

        RdmaChannel channel = getConnectedChannel();
        if (channel == null) {
            /*
             * Connection is unavailable. We will drop pending messages and let at-least-once message replay kick in.
             *
             * Another option would be to buffer the messages in memory.  But this option has the risk of causing OOM errors,
             * especially for topologies that disable message acking because we don't know whether the connection recovery will
             * succeed  or not, and how long the recovery will take.
             */
            dropMessages(msgs);
            return;
        }

        synchronized (writeLock) {
            MessageBatch full = batcher.add(msgs);
            if(full != null){
                flushMessages(channel, full);
            }
        }
    }

    private void flushMessages(RdmaChannel channel, MessageBatch full) {
        try {
            ChannelBuffer buffer = full.buffer();
            ByteBuffer byteBuffer = buffer.toByteBuffer();
            rdmaChannel.setDataBuffer(byteBuffer);

            /////////////////////////////////////////////////RDMA flushMessages////////////////////////////////////////////////
            RdmaBuffer sendMr = rdmaChannel.getSendBuffer();
            ByteBuffer sendBuf = sendMr.getByteBuffer();
            RdmaBuffer dataMr = rdmaChannel.getDataBuffer();
            ByteBuffer dataBuf = dataMr.getByteBuffer();
            RdmaBuffer recvMr = rdmaChannel.getReceiveBuffer();
            ByteBuffer recvBuf = recvMr.getByteBuffer();

            sendBuf.putLong(dataMr.getAddress());
            sendBuf.putInt(dataMr.getLkey());
            sendBuf.putInt(dataMr.getLength());
            sendBuf.clear();

            LOG.info("first add: " + dataMr.getAddress() + " lkey: " + dataMr.getLkey() + " length: " + dataMr.getLength());
            LOG.info("dataBuf: " + dataBuf.asCharBuffer().toString());
            LOG.info("byteBuffer: " + byteBuffer.asCharBuffer().toString());

            //post a send call, here we send a message which include the RDMA information of a data buffer
            recvBuf.clear();
            dataBuf.clear();
            sendBuf.clear();
            rdmaChannel.rdmaSendInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf) {
                    LOG.info("RDMA SEND Address Success");
                }

                @Override
                public void onFailure(Throwable exception) {
                    exception.printStackTrace();
                }
            }, new long[]{sendMr.getAddress()}, new int[]{sendMr.getLkey()}, new int[]{sendMr.getLength()});

            LOG.info("RDMA SEND Address Success");

            System.out.println("VerbsServer::stag info sent");

            //initSGRecv
            rdmaChannel.initRecvs();

            //wait for the final message from the server
            rdmaChannel.completeSGRecv();

            System.out.println("VerbsServer::done");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean hasMessages(WorkerMessage workerMessage) {
        return workerMessage != null && workerMessage.tasks().size()!=0;
    }

    private void dropMessages(WorkerMessage msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
    }

    private int iteratorSize(WorkerMessage msgs) {
        return msgs.tasks().size();
    }


    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public void close() {
        if (!closing) {
            LOG.info("closing Netty Client {}", dstAddressPrefixedName);
            context.removeClient(dstHost, dstAddress.getPort());
            // Set closing to true to prevent any further reconnection attempts.
            closing = true;
            waitForPendingMessagesToBeSent();
            closeChannel();
        }
    }

    private void closeChannel() {
        RdmaChannel channel = channelRef.get();
        if (channel != null) {
            try {
                channel.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
        }
    }

    private void waitForPendingMessagesToBeSent() {
        LOG.info("waiting up to {} ms to send {} pending messages to {}",
                PENDING_MESSAGES_FLUSH_TIMEOUT_MS, pendingMessages.get(), dstAddressPrefixedName);
        long totalPendingMsgs = pendingMessages.get();
        long startMs = System.currentTimeMillis();
        while (pendingMessages.get() != 0) {
            try {
                long deltaMs = System.currentTimeMillis() - startMs;
                if (deltaMs > PENDING_MESSAGES_FLUSH_TIMEOUT_MS) {
                    LOG.error("failed to send all pending messages to {} within timeout, {} of {} messages were not " +
                            "sent", dstAddressPrefixedName, pendingMessages.get(), totalPendingMsgs);
                    break;
                }
                Thread.sleep(PENDING_MESSAGES_FLUSH_INTERVAL_MS);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public Object getState() {
        LOG.debug("Getting metrics for client connection to {}", dstAddressPrefixedName);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("reconnects", totalConnectionAttempts.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("pending", pendingMessages.get());
        ret.put("lostOnSend", messagesLost.getAndSet(0));
        ret.put("dest", dstAddress.toString());
        String src = srcAddressName();
        if (src != null) {
            ret.put("src", src);
        }
        return ret;
    }

    private String srcAddressName() {
        String name = null;
        RdmaChannel channel = channelRef.get();
        if (channel != null) {
            name=IBAddress;
        }
        return name;
    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    private boolean reconnectingAllowed() {
        return !closing;
    }

    /**
     * Asynchronously establishes a RDMA connection to the remote address
     * This task runs on a single thread shared among all clients, and thus
     * should not perform operations that block.
     */
    private class Connect implements TimerTask {

        private final InetSocketAddress address;

        public Connect(InetSocketAddress address) {
            this.address = address;
        }

        private void reschedule(Throwable t) {
            String baseMsg = String.format("connection attempt %s to %s failed", connectionAttempts,
                    dstAddressPrefixedName);
            String failureMsg = (t == null) ? baseMsg : baseMsg + ": " + t.toString();
            LOG.error(failureMsg);
            long nextDelayMs = retryPolicy.getSleepTimeMs(connectionAttempts.get(), 0);
            scheduleConnect(nextDelayMs);
        }


        @Override
        public void run(Timeout timeout) throws Exception {
            if (reconnectingAllowed()) {
                try {
                    final int connectionAttempt = connectionAttempts.getAndIncrement();
                    totalConnectionAttempts.getAndIncrement();

                    rdmaChannel=rdmaClient.getRdmaChannel(address, true);

                    if(rdmaChannel!=null && connectionEstablished(rdmaChannel)){
                        boolean setChannel = channelRef.compareAndSet(null, rdmaChannel);
                        checkState(setChannel);
                        LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), rdmaChannel.toString(),
                                connectionAttempt);
                        if (messagesLost.get() > 0) {
                            LOG.warn("Re-connection to {} was successful but {} messages has been lost so far", address.toString(), messagesLost.get());
                        }
                    }
                } catch (Exception cause) {
                    cause.printStackTrace();
                    reschedule(cause);
                    if (rdmaChannel != null) {
                        rdmaChannel.stop();
                    }
                }
            } else {
                close();
                throw new RuntimeException("Giving up to scheduleConnect to " + dstAddressPrefixedName + " after " +
                        connectionAttempts + " failed attempts. " + messagesLost.get() + " messages were lost");

            }
        }
    }

    private static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }
}
