package org.apache.storm;

import org.apache.storm.model.MyTuple;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/23.
 */
public class TupleCreateTest {
    @Test
    public void test(){
        MyTuple tuple=new MyTuple(12,"ack_stream",new Values("tanjie"));
        KryoValuesSerializer kryoValuesSerializer=new KryoValuesSerializer(Utils.readDefaultConfig());
        kryoValuesSerializer.serializeObject(tuple);
    }
}
