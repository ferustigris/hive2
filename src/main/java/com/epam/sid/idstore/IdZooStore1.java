package com.epam.sid.idstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Implements storing IDs in zookeeper
 */
public class IdZooStore1 implements IdStore, Watcher {
    private ZooKeeper zk;
    Stat st = new Stat();
    private static final Log log = LogFactory.getLog(IdZooStore1.class.getName());
    private int attemptCount = 3;

    public IdZooStore1() {
        String host = "localhost";
        int port = 2181;
        try {
            zk = new ZooKeeper(host, port, this);
        } catch (IOException e) {
            String msg = "Can't connect to ZooKeeper(" + host + ", " + port + ")";
            log.error(msg, e);
            throw new RuntimeException(msg);
        }
    }

    public long getNextId(String sequenceName) {
        try {
            return getId(sequenceName);
        } catch (InterruptedException e) {
            String msg = "Can't get next key (Interrupted)";
            log.error(msg, e);
            throw new RuntimeException(msg);
        } catch (KeeperException e) {
            String msg = "Can't get next key (Failed connection to ZooKeeper, " + e.code().toString() + ")";
            log.error(msg, e);
            throw new RuntimeException(msg);
        }
    }

    private long getId(String sequenceName) throws KeeperException, InterruptedException {
        final String node = "/uid/" + sequenceName;
        createIfNotExists(node);
        long id = getAndUpdate(node);
        return id;
    }

    private void createIfNotExists(String node) throws KeeperException, InterruptedException {
        if (zk.exists(node, false) == null) {
            zk.create(node, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private long getAndUpdate(String node) throws KeeperException, InterruptedException {
        for(int attempt = 0; attempt < attemptCount; ++attempt) {
            try {
                return tryGetAndUpdate(node);
            } catch (KeeperException e) {
                if (!e.code().equals(KeeperException.Code.BADVERSION)) {
                    log.error("Can't perform tryGetAndUpdate operation", e);
                    throw e;
                }
            }
        }
        throw new RuntimeException("Too much attempts to get and update operations");
    }

    private long tryGetAndUpdate(String node) throws KeeperException, InterruptedException {
        // Get current value
        byte[] uid = zk.getData(node, false, st);
        long id = Long.parseLong(new String(uid));
        uid = String.valueOf(id).getBytes();
        // Update value
        zk.setData(node, uid, st.getVersion());
        return id;
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            String msg = "Can't close connection to ZooKeeper)";
            log.error(msg, e);
            throw new RuntimeException(msg);
        }
    }

    public void process(WatchedEvent watchedEvent) {

    }
}
