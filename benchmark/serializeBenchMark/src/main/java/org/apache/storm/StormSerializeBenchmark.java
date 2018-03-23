package org.apache.storm;

import org.apache.storm.model.MyTuple;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/23.
 * 序列化测试 CPU利用率BenchMark
 * java -cp serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.StormSerializeBenchmark didi 1
 */
public class StormSerializeBenchmark {
    private static String DidDiOrdersMatch="12945ba4e5c2499433e2dc7b7b4cad15,1478922635,5fabb7d49469e7aef524bf044f2ca4eb";
    private static String NASDAQStockDeal="FLWS\t16:13:43\t10.75\t2 - Cancelled Trade";
    public static void main(String[] args) {
        String dataType = args[0];
        Integer serializeTimes=Integer.valueOf(args[1]);
        MyTuple tuple=null;
        if(dataType.equals("didi"))
            tuple=new MyTuple(12,"ack_stream",new Values(DidDiOrdersMatch));
        else if(dataType.equals("nasdaq"))
            tuple=new MyTuple(12,"ack_stream",new Values(NASDAQStockDeal));

        KryoValuesSerializer kryoValuesSerializer=new KryoValuesSerializer(Utils.readDefaultConfig());
        for(int i=0;i<serializeTimes;i++){
            byte[] bytes = kryoValuesSerializer.serializeObject(tuple);
        }
    }
}
