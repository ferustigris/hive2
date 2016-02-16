package com.epam.sid.idstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * Implements storing IDs in zookeeper
 */
public class IdZooStore implements IdStore {
    private ZooKeeper zk;
    Stat st = new Stat();
    private static final Log log = LogFactory.getLog(IdZooStore.class.getName());
    private int attemptCount;

    public IdZooStore(ZooKeeper zoo, int attemptCount) {
        this.attemptCount = attemptCount;
        this.zk = zoo;
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
        long id = Long.parseLong(new String(uid)) + 1;
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

}
