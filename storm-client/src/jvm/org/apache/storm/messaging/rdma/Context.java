package org.apache.storm.messaging.rdma;

import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;

import java.util.HashMap;
import java.util.Map;

/**
 * locate org.apache.storm.messaging.rdma
 * Created by mastertj on 2018/9/11.
 */
public class Context implements IContext {
    private Map<String, Object> topoConf;
    private Map<String, IConnection> connections;

    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.topoConf=topoConf;
        connections=new HashMap<>();
    }

    /**
     * terminate this context
     */
    @Override
    public void term() {
        for (IConnection conn : connections.values()) {
            conn.close();
        }

        connections = null;
    }

    /**
     * establish a server with a binding port
     */
    @Override
    public IConnection bind(String storm_id, int port) {
        return null;
    }

    /**
     * establish a connection to a remote server
     */
    @Override
    public IConnection connect(String storm_id, String host, int port){
        return null;
    }

    synchronized void removeClient(String host, int port) {
        if (connections != null) {
            connections.remove(key(host, port));
        }
    }

    private String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
