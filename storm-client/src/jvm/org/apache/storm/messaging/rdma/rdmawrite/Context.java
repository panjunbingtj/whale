package org.apache.storm.messaging.rdma.rdmawrite;

import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.rdma.Server;

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
        IConnection server = null;
        try {
            server = new Server(topoConf, port);
            connections.put(key(storm_id, server.getPort()), server);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    @Override
    public IConnection connect(String storm_id, String host, int port){
        IConnection client=null;
        try {
            IConnection connection = connections.get(key(host,port));
            if(connection !=null)
            {
                return connection;
            }
            client = new Client(topoConf, host, port, this);
            connections.put(key(host, client.getPort()), client);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
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
