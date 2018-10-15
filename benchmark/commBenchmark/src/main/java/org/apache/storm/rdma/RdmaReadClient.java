package org.apache.storm.rdma;

import com.ibm.disni.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/8/27.
 * java -cp commBenchmark-2.0.0-SNAPSHOT.jar org.apache.storm.rdma.RdmaReadClient didi 1
 */
public class RdmaReadClient {

    private static final Logger logger = LoggerFactory.getLogger(RdmaReadClient.class);

    private static String DidDiOrdersMatch="12945ba4e5c2499433e2dc7b7b4cad15,1478922635,5fabb7d49469e7aef524bf044f2ca4eb";
    private static String NASDAQStockDeal="FLWS\t16:13:43\t10.75\t2 - Cancelled Trade";

    public static void main(String[] args) throws Exception {

        String app= args[0];
        Integer parallelism=Integer.valueOf(args[1]);

        StringBuilder records = new StringBuilder();
        if(app.equals("didi")){
            records.append(DidDiOrdersMatch);
        }else if(app.equals("nasdaq")){
            records.append(NASDAQStockDeal);
        }
        for (int i = 0; i < parallelism; i++) {
            records.append("22");
        }

        RdmaNode rdmaClient=new RdmaNode("10.10.0.24", true, new RdmaShuffleConf(), new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("success1111");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        }, (remote, rdmaChannel) -> {

        });

        RdmaChannel rdmaChannel = rdmaClient.getRdmaChannel(new InetSocketAddress("10.10.0.25", 1955), true);

        InetSocketAddress address = null;

        VerbsTools commRdma = rdmaChannel.getCommRdma();

        RdmaBuffer sendMr = rdmaChannel.getSendBuffer();
        ByteBuffer sendBuf = sendMr.getByteBuffer();

        RdmaBuffer recvMr = rdmaChannel.getReceiveBuffer();
        ByteBuffer recvBuf = recvMr.getByteBuffer();

        //dataBuf.asCharBuffer().put("This is a RDMA/read on stag !");

        long startTimeMillis = System.nanoTime();

        //initSGRecv
        rdmaChannel.initRecvs();

        ByteBuffer byteBuffer= ByteBuffer.allocateDirect(records.toString().getBytes().length);

        byteBuffer.put(records.toString().getBytes());
        byteBuffer.clear();
        rdmaChannel.setDataBuffer(byteBuffer);

        RdmaBuffer dataMr = rdmaChannel.getDataBuffer();
        ByteBuffer dataBuf = dataMr.getByteBuffer();

        sendBuf.clear();
        sendBuf.putLong(dataMr.getAddress());
        sendBuf.putInt(dataMr.getLkey());
        sendBuf.putInt(dataMr.getLength());
        sendBuf.clear();

        //logger.info("first add: " + dataMr.getAddress() + " lkey: " + dataMr.getLkey() + " length: " + dataMr.getLength());
        //logger.info("dataBuf: " + dataBuf.asCharBuffer().toString());
        //logger.info("sendBuf: " + sendBuf.getLong()+" "+sendBuf.getInt()+" "+sendBuf.getInt());

        //post a send call, here we send a message which include the RDMA information of a data buffer
        recvBuf.clear();
        dataBuf.clear();
        sendBuf.clear();
        rdmaChannel.rdmaSendInQueue(new RdmaCompletionListener() {
            @Override
            public void onSuccess(ByteBuffer buf) {
                logger.info("RDMA SEND Address Success");
            }

            @Override
            public void onFailure(Throwable exception) {
                exception.printStackTrace();
            }
        }, new long[]{sendMr.getAddress()}, new int[]{sendMr.getLkey()}, new int[]{sendMr.getLength()});

        //rdmaChannel.completeSGRecv();

        //logger.info("RDMA SEND Address Success");

        System.out.println("VerbsServer::stag info sent");

        //wait for the final message from the server
        rdmaChannel.completeSGRecv();

        //System.out.println("VerbsServer::done");

        long endTimeMillis = System.nanoTime();
        System.out.println("Total Time: "+ (endTimeMillis-startTimeMillis));
    }
}
