package org.apache.storm.serializer;

import com.esotericsoftware.kryo.io.Output;
import org.apache.storm.model.MyTuple;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.utils.Utils;

import java.io.IOException;

/**
 * locate org.apache.storm.serializer
 * Created by mastertj on 2018/3/26.
 */
public class KryoMyTupleSerializer {
	private KryoValuesSerializer _kryo;
	private Output _kryoOut;

	public KryoMyTupleSerializer() {
		_kryo = new KryoValuesSerializer(Utils.readDefaultConfig());
		_kryoOut = new Output(2000, 2000000000);
	}

	public byte[] serialize(MyTuple tuple) {
		try {

			_kryoOut.clear();
			_kryoOut.writeInt(tuple.getTaskid(), true);
			_kryoOut.writeString(tuple.getStreamId());
			_kryo.serializeInto(tuple.getValues(), _kryoOut);
			return _kryoOut.toBytes();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
