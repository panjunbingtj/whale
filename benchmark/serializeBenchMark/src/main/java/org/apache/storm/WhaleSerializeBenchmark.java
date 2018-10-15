package org.apache.storm;

import org.apache.storm.model.WhaleTuple;
import org.apache.storm.serializer.KryoMyTupleSerializer;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/23.
 * 序列化测试 CPU利用率BenchMark
 * java -cp serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.WhaleSerializeBenchmark didi 10000 1 30
 -Dcom.sun.management.jmxremote.port=18999
 -Dcom.sun.management.jmxremote.ssl=false
 -Dcom.sun.management.jmxremote.authenticate=false
 * java -cp serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.WhaleSerializeBenchmark nasdaq 10000 1 30
 */
public class WhaleSerializeBenchmark {
    private static String DidDiOrdersMatch="12945ba4e5c2499433e2dc7b7b4cad15,1478922635,5fabb7d49469e7aef524bf044f2ca4eb";
    private static String NASDAQStockDeal="FLWS\t16:13:43\t10.75\t2 - Cancelled Trade";
    public static void main(String[] args) {
        String dataType = args[0];
        Integer tuplesNum=Integer.valueOf(args[1]);
        Integer serializeTimes=Integer.valueOf(args[2]);
        Integer parallelism=Integer.valueOf(args[2]);

        List<Integer> taskids=new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            taskids.add(12);
        }
        WhaleTuple tuple=null;
        if(dataType.equals("didi"))
            tuple=new WhaleTuple(taskids,"ack_stream",new Values(DidDiOrdersMatch));
        else if(dataType.equals("nasdaq"))
            tuple=new WhaleTuple(taskids,"ack_stream",new Values(NASDAQStockDeal));


        KryoMyTupleSerializer _kryo=new KryoMyTupleSerializer();
        long startTimeMillis = System.nanoTime();
        for(int i=0;i<serializeTimes*tuplesNum;i++){
            byte[] bytes = _kryo.serializeWhaleTuple(tuple);
        }
        long endTimeMillis = System.nanoTime();
        System.out.println("Total Time: "+ (endTimeMillis-startTimeMillis));
    }
}
