package org.apache.storm.messaging.netty;

import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * locate org.apache.storm.messaging.netty
 * Created by mastertj on 2018/8/23.
 */
public class RDMAClient extends ConnectionWithStatus implements IStatefulObject, ISaslClient {
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private static final long NO_DELAY_MS = 0L;
    private static final Timer timer = new Timer("Netty-ChannelAlive-Timer", true);

    private final Map<String, Object> topoConf;
    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private final ClientBootstrap bootstrap;
    private final InetSocketAddress dstAddress;
    protected final String dstAddressPrefixedName;
    //The actual name of the host we are trying to connect to so that
    // when we remove ourselves from the connection cache there is no concern that
    // the resolved host name is different.
    private final String dstHost;
    private volatile Map<Integer, Double> serverLoad = null;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<Channel> channelRef = new AtomicReference<>();

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

    /**
     * Whether the SASL channel is ready.
     */
    private final AtomicBoolean saslChannelReady = new AtomicBoolean(false);

    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

    private final Context context;

    private final HashedWheelTimer scheduler;

    private final MessageBuffer batcher;

    private final Object writeLock = new Object();

    public RDMAClient(Map<String, Object> topoConf, ChannelFactory factory, HashedWheelTimer scheduler, String host, int port, Context context) {
        this.topoConf = topoConf;
        closing = false;
        this.scheduler = scheduler;
        this.context = context;
        int bufferSize = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        // if SASL authentication is disabled, saslChannelReady is initialized as true; otherwise false
        saslChannelReady.set(!ObjectReader.getBoolean(topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION), false));
        LOG.info("creating Netty Client, connecting to {}:{}, bufferSize: {}", host, port, bufferSize);
        int messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);

        int maxReconnectionAttempts = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        // Initiate connection to remote destination
        bootstrap = createClientBootstrap(factory, bufferSize, topoConf);
        dstHost = host;
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);
        launchChannelAliveThread();
        scheduleConnect(NO_DELAY_MS);
        batcher = new MessageBuffer(messageBatchSize);
    }

    @Override
    public Status status() {
        return null;
    }

    @Override
    public void registerRecv(IConnectionCallback cb) {

    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {

    }

    @Override
    public void send(int taskId, byte[] payload) {

    }

    @Override
    public void send(WorkerMessage msgs) {

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

    }

    @Override
    public void channelConnected(Channel channel) {

    }

    @Override
    public void channelReady() {

    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String secretKey() {
        return null;
    }

    @Override
    public Object getState() {
        return null;
    }
}
