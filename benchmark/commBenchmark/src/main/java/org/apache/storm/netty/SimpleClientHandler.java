package org.apache.storm.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.storm.model.MyTuple;
import org.apache.storm.serializer.KryoMyTupleSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;

public class SimpleClientHandler extends ChannelInboundHandlerAdapter {
    public static Logger logger = LoggerFactory.getLogger(SimpleClientHandler.class);

    private static String DidDiOrdersMatch="12945ba4e5c2499433e2dc7b7b4cad15,1478922635,5fabb7d49469e7aef524bf044f2ca4eb";
    private static String NASDAQStockDeal="FLWS\t16:13:43\t10.75\t2 - Cancelled Trade";
    private static KryoMyTupleSerializer kryoMyTupleSerializer=new KryoMyTupleSerializer();;

    private int parallesimNum;

    public SimpleClientHandler(int parallesimNum) {
        this.parallesimNum = parallesimNum;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("SimpleClientHandler.channelRead");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // 当出现异常就关闭连接
        cause.printStackTrace();
        ctx.close();
    }


    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("startTimeMills:" + System.currentTimeMillis());
        for (int i = 0; i <parallesimNum; i++) {
            String msg = DidDiOrdersMatch+"\n";
            MyTuple tuple = new MyTuple(12,"ack_stream", Arrays.asList(msg));

            BufferedReader in2 = new BufferedReader(new FileReader("/proc/stat"));
            String line=null;
            long idleCpuTime1 = 0, totalCpuTime1 = 0;	//分别为系统启动后空闲的CPU时间和总的CPU时间
            while((line=in2.readLine()) != null){
                if(line.startsWith("cpu")){
                    line = line.trim();
                    logger.debug(line);
                    String[] temp = line.split("\\s+");
                    idleCpuTime1 = Long.parseLong(temp[4]);
                    for(String s : temp){
                        if(!s.equals("cpu")){
                            totalCpuTime1 += Long.parseLong(s);
                        }
                    }
                    logger.debug("IdleCpuTime: " + idleCpuTime1 + ", " + "TotalCpuTime" + totalCpuTime1);
                    break;
                }
            }

            byte[] bytes = kryoMyTupleSerializer.serialize(tuple);

            long idleCpuTime2 = 0, totalCpuTime2 = 0;	//分别为系统启动后空闲的CPU时间和总的CPU时间
            while((line=in2.readLine()) != null){
                if(line.startsWith("cpu")){
                    line = line.trim();
                    logger.debug(line);
                    String[] temp = line.split("\\s+");
                    idleCpuTime2 = Long.parseLong(temp[4]);
                    for(String s : temp){
                        if(!s.equals("cpu")){
                            totalCpuTime2 += Long.parseLong(s);
                        }
                    }
                    logger.debug("IdleCpuTime: " + idleCpuTime2 + ", " + "TotalCpuTime" + totalCpuTime2);
                    break;
                }
            }

            ctx.write(bytes);
            ctx.flush();
        }
    }

}
