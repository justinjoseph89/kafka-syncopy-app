package com.kafka.copy.app.zkutilsimpl;

import static com.kafka.copy.app.constants.CommonConstants.INDEX_MAPPER;
import static com.kafka.copy.app.constants.ZkConstatnts.ZNODE_PREFIX;
import static com.kafka.copy.app.constants.ZkConstatnts.ZNODE_START;
import static com.kafka.copy.app.constants.ZkConstatnts.ZNODE_SEPERATOR;
import static com.kafka.copy.app.constants.ZkConstatnts.ZNODE_DEFAULT_VALUE;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.kafkautilsimpl.KafkaConnection;
import com.kafka.copy.app.zkutils.ZkConnect;;

/**
 * @author justin
 *
 */
public class ZkConnectImpl extends ZkConnect {
  private static final Logger logger = Logger.getLogger(ZkConnectImpl.class.getName());
  private ZooKeeper zk;
  private CountDownLatch connSignal = new CountDownLatch(0);
  private String znodeName;
  private KafkaConfigReader configReader;
  @SuppressWarnings("unused")
  private boolean reset;

  /**
   * @param topicName
   * @param reset
   * @param configReader
   */
  public ZkConnectImpl(final String topicName, final boolean reset, final KafkaConfigReader configReader) {
    this.configReader = configReader;
    this.znodeName = ZNODE_PREFIX + ZNODE_START + configReader.getAppVersion() + ZNODE_SEPERATOR + topicName;
    this.zk = this.connect(configReader.getZookeeperHost());
    this.reset = reset;
    createNode(topicName);
  }

  @Override
  protected ZooKeeper connect(final String zkHost) {
    try {
      zk = new ZooKeeper(zkHost, 3000, new Watcher() {
        public void process(WatchedEvent event) {
          if (event.getState() == KeeperState.SyncConnected) {
            connSignal.countDown();
          }
        }
      });
      connSignal.await();
    } catch (IOException | InterruptedException e) {
      logger.error("Exception while getting connection of the zookeeper node.. " + e.getMessage());
    }
    return zk;
  }

  @Override
  public void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      logger.error("Exception while closing connection of the zookeeper node.. " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  protected <K, V> void createNode(final String topicName) {
    final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
        KafkaConnection.getSourceProducerProperties(this.configReader));

    producer.partitionsFor(topicName).forEach(partitionInfo -> {
      final int partitionNumber = partitionInfo.partition();
      try {
        final String znodeUpdated = this.znodeName + INDEX_MAPPER + partitionNumber;
        final Stat nodeExistence = zk.exists(znodeUpdated, true);
        if (nodeExistence != null) {
          if (this.reset = true) {
            zk.delete(znodeUpdated, zk.exists(znodeUpdated, true).getVersion());
            zk.create(znodeUpdated, String.valueOf(ZNODE_DEFAULT_VALUE).getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
          } else {
            logger.warn("Node Already Present. Do Nothing. Go For Update. Please Ignore If it is intended");
          }
        } else {
          zk.create(znodeUpdated, String.valueOf(ZNODE_DEFAULT_VALUE).getBytes(), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        }
      } catch (KeeperException | InterruptedException e) {
        logger.error("Exception while creating the zookeeper node.. " + e.getMessage());
      }
    });
    producer.close();

  }

  @Override
  public void updateNode(final double maxTime, final int partition) {
    final String znodeUpdated = this.znodeName + INDEX_MAPPER + partition;
    try {
      zk.setData(znodeUpdated, String.valueOf(maxTime).getBytes(), zk.exists(znodeUpdated, true).getVersion());
    } catch (KeeperException | InterruptedException e) {
      logger.error("Exception while updating data in the zookeeper node.. " + e.getMessage());
    }
  }

  @Override
  protected byte[] getDataFromPath(String zNode) {
    byte[] retrivedData = null;
    try {
      retrivedData = zk.getData(zNode, true, zk.exists(zNode, true));
    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
    return retrivedData;
  }

  @Override
  public double getMinimum(final HashMap<String, Long> zeroLag) {
    final TreeSet<Double> dataTreeSet = new TreeSet<Double>();
    final TreeSet<Double> backupDataTreeSet = new TreeSet<Double>();

    final List<String> zNodes;
    try {
      zNodes = zk.getChildren(ZNODE_PREFIX, true);
      zNodes.forEach(zNode -> {
        if (zNode.startsWith(ZNODE_START + this.configReader.getAppVersion())) {
          final String[] topicPartitionArr = zNode.split(ZNODE_SEPERATOR);
          final String topicPartitionName = topicPartitionArr[topicPartitionArr.length - 1];
          final String data = new String(getDataFromPath(ZNODE_PREFIX + zNode));
          if (!zeroLag.containsKey(topicPartitionName)) {
            dataTreeSet.add(Double.parseDouble(data));
          }
          backupDataTreeSet.add(Double.parseDouble(data));
        }
      });

    } catch (KeeperException | InterruptedException e) {
      logger.error("Exception while getting minimum value of the node.. " + e.getMessage());
    }
    return dataTreeSet.isEmpty() ? backupDataTreeSet.isEmpty() ? 0d : backupDataTreeSet.ceiling(100d)
        : dataTreeSet.ceiling(100d);
  }

  @Override
  public double getMinimum() {
    final TreeSet<Double> dataTreeSet = new TreeSet<Double>();
    final List<String> zNodes;
    try {
      zNodes = zk.getChildren(ZNODE_PREFIX, true);
      zNodes.forEach(zNode -> {
        if (zNode.startsWith(ZNODE_START + this.configReader.getAppVersion())) {
          final String data = new String(getDataFromPath(ZNODE_PREFIX + zNode));
          dataTreeSet.add(Double.parseDouble(data));
        }
      });
    } catch (KeeperException | InterruptedException e) {
      logger.error("Exception while getting minimum value of the node.. " + e.getMessage());
    }
    return dataTreeSet.first() == 0d ? 0d : dataTreeSet.ceiling(100d);
  }

}