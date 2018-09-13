package org.apache.storm.messaging.rdma;

import com.ibm.disni.channel.*;
import com.ibm.disni.rdma.verbs.IbvSendWR;
import com.ibm.disni.rdma.verbs.IbvSge;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/9/11.
 */
public class Server extends ConnectionWithStatus implements IStatefulObject {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    @SuppressWarnings("rawtypes")
    Map<String, Object> topoConf;
    int port;
    private final ConcurrentHashMap<String, AtomicInteger> messagesEnqueued = new ConcurrentHashMap<>();
    private final AtomicInteger messagesDequeued = new AtomicInteger(0);

    private volatile boolean closing = false;
    List<TaskMessage> closeMessage = Arrays.asList(new TaskMessage(-1, null));
    private KryoValuesSerializer _ser;
    private IConnectionCallback _cb = null;
    //private final int boundPort;

    ///RDMA HOSTNAME
    private final String IBAddress;

    private RdmaNode rdmaServer;
    private RdmaChannel rdmaChannel;

    public Server(Map<String, Object> topoConf, int port) throws Exception {
        this.topoConf = topoConf;
        this.port = port;

        ///add IBAddress Configure
        BufferedReader bufferedReader=new BufferedReader(new FileReader("/whale/RDMAHostName"));
        this.IBAddress=bufferedReader.readLine();

        rdmaServer=new RdmaNode(IBAddress, false, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                LOG.info("success1111");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        }, (remote, rdmaChannel) -> {
                VerbsTools commRdma = rdmaChannel.getCommRdma();

            ByteBuffer recvBuf = null;
            RdmaBuffer dataMr = null;
            ByteBuffer dataBuf = null;
            RdmaBuffer sendMr = null;
            ByteBuffer sendBuf = null;
            try {
                RdmaBuffer recvMr = rdmaChannel.getReceiveBuffer();
                recvBuf = recvMr.getByteBuffer();
                dataMr = rdmaChannel.getDataBuffer();
                dataBuf = dataMr.getByteBuffer();
                sendMr = rdmaChannel.getSendBuffer();
                sendBuf = sendMr.getByteBuffer();
            } catch (IOException e) {
                e.printStackTrace();
            }

            LOG.info("first add: "+recvBuf.getLong()+" lkey: "+recvBuf.getInt()+" length: "+recvBuf.getInt());

                while (true) {
                    try {
                        //initSGRecv
                        rdmaChannel.initRecvs();
                        //let's wait for the first message to be received from the server
                        rdmaChannel.completeSGRecv();

                        recvBuf.clear();
                        dataBuf.clear();
                        long addr = recvBuf.getLong();
                        int lkey = recvBuf.getInt();
                        int length = recvBuf.getInt();
                        LOG.info("second add: " + addr + " lkey: " + lkey + " length: " + length);
                        LOG.info("second add: " + dataBuf.getLong() + " lkey: " + dataBuf.getInt() + " length: " + dataBuf.getInt());

                        recvBuf.clear();
                        dataBuf.clear();
                        sendBuf.clear();
                        rdmaChannel.rdmaReadInQueue(new RdmaCompletionListener() {
                            @Override
                            public void onSuccess(ByteBuffer buf) {
                                LOG.info("RdmaActiveReadClient::read memory from server: " + buf.asCharBuffer().toString());
                            }

                            @Override
                            public void onFailure(Throwable exception) {
                                exception.printStackTrace();
                            }
                        }, dataMr.getAddress(), dataMr.getLkey(), new int[]{length}, new long[]{addr}, new int[]{lkey});

                        //let's prepare a one-sided RDMA read operation to fetch the content of that remote buffer
                        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
                        IbvSge sgeSend = new IbvSge();

                        dataBuf.clear();
                        LOG.info(dataBuf.toString());
                        LOG.info("RdmaActiveReadClient::read memory from server: " + dataBuf.asCharBuffer().toString());

                        WorkerMessage workerMessage=new WorkerMessage();
                        workerMessage.deserialize(dataBuf);
                        received(workerMessage,remote,rdmaChannel);

                        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
                        sgeList.add(sgeSend);
                        IbvSendWR sendWR = new IbvSendWR();
                        sgeSend = new IbvSge();
                        sgeSend.setAddr(sendMr.getAddress());
                        sgeSend.setLength(sendMr.getLength());
                        sgeSend.setLkey(sendMr.getLkey());
                        sgeList.clear();
                        sgeList.add(sgeSend);
                        sendWR = new IbvSendWR();
                        sendWR.setWr_id(1002);
                        sendWR.setSg_list(sgeList);
                        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
                        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
                        wrList_send.clear();
                        wrList_send.add(sendWR);

                        //let's post the final message
                        recvBuf.clear();
                        dataBuf.clear();
                        sendBuf.clear();
                        commRdma.send(wrList_send, true, false);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
        });


        // Bind and start to accept incoming connections.
    }

    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(rdmaServer.passiveRdmaChannelMap.values().iterator())) {
            return Status.Connecting;
        } else {
            if (rdmaChannel.isConnected()) {
                return Status.Ready;
            } else {
                return Status.Connecting; // need to wait until sasl channel is also ready
            }
        }
    }

    private boolean connectionEstablished(RdmaChannel channel) {
        return channel != null && channel.isConnected();
    }


    private boolean connectionEstablished(Iterator<RdmaChannel> iterator) {
        boolean allEstablished = true;
       while (iterator.hasNext()){
            if (!(connectionEstablished(iterator.next()))) {
                allEstablished = false;
                break;
            }
        }
        return allEstablished;
    }
    @Override
    public void registerRecv(IConnectionCallback cb) {
        _cb = cb;
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
//        try {
//            MessageBatch mb = new MessageBatch(1);
//            mb.add(new WorkerMessage(Arrays.asList(-1), _ser.serialize(Arrays.asList((Object)taskToLoad))));
//            allChannels.write(mb);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

    }

    @Override
    public void send(int taskId, byte[] payload) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public void send(WorkerMessage msgs) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        return null;
    }

    @Override
    public int getPort() {
        return 1955;
    }

    @Override
    public void close() {
        Iterator<RdmaChannel> iterator = rdmaServer.passiveRdmaChannelMap.values().iterator();
        while (iterator.hasNext()){
            RdmaChannel rdmaChannel = iterator.next();
            try {
                rdmaChannel.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Object getState() {
        LOG.debug("Getting metrics for server on port {}", port);
        HashMap<String, Object> ret = new HashMap<>();
        ret.put("dequeuedMessages", messagesDequeued.getAndSet(0));
        HashMap<String, Integer> enqueued = new HashMap<String, Integer>();
        Iterator<Map.Entry<String, AtomicInteger>> it = messagesEnqueued.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, AtomicInteger> ent = it.next();
            //Yes we can delete something that is not 0 because of races, but that is OK for metrics
            AtomicInteger i = ent.getValue();
            if (i.get() == 0) {
                it.remove();
            } else {
                enqueued.put(ent.getKey(), i.getAndSet(0));
            }
        }
        ret.put("enqueued", enqueued);

        // Report messageSizes metric, if enabled (non-null).
        if (_cb instanceof IMetric) {
            Object metrics = ((IMetric) _cb).getValueAndReset();
            if (metrics instanceof Map) {
                ret.put("messageBytes", metrics);
            }
        }

        return ret;
    }

    public void received(Object message, String remote, RdmaChannel channel)  throws InterruptedException {
        List<WorkerMessage> messages = (List<WorkerMessage>) message;
        LOG.debug("Server received WorkerMessage : {}",messages);
        enqueue(messages, remote);
    }

    /**
     * enqueue a received message
     * @throws InterruptedException
     */
    protected void enqueue(List<WorkerMessage> msgs, String from) throws InterruptedException {
        if (null == msgs || msgs.size() == 0 || closing) {
            return;
        }
        addReceiveCount(from, msgs.size());
        if (_cb != null) {
            _cb.recv(msgs);
        }
    }

    private void addReceiveCount(String from, int amount) {
        //This is possibly lossy in the case where a value is deleted
        // because it has received no messages over the metrics collection
        // period and new messages are starting to come in.  This is
        // because I don't want the overhead of a synchronize just to have
        // the metric be absolutely perfect.
        AtomicInteger i = messagesEnqueued.get(from);
        if (i == null) {
            i = new AtomicInteger(amount);
            AtomicInteger prev = messagesEnqueued.putIfAbsent(from, i);
            if (prev != null) {
                prev.addAndGet(amount);
            }
        } else {
            i.addAndGet(amount);
        }
    }
}
