package com.kafka.copy.app.zkutils;

import java.util.HashMap;

import org.apache.zookeeper.ZooKeeper;

/**
 * @author justin
 *
 */
public abstract class ZkConnect {

  /**
   * Create Zookeeper client object to get access the zookeeper related apis.
   * Since kafka is deprecating zookeeper soon should I change this one?
   * 
   * @param host host name for zookeeper
   * @return ZooKeeper object
   */
  abstract protected ZooKeeper connect(final String host);

  /**
   * Close the zookeeper connection once it is done using it
   * 
   * @return returns void
   */
  abstract public void close();

  /**
   * Create the node with default data in the initialization itself to avoid the
   * future issues
   * 
   * @param <K>       key type
   * @param <V>       value type
   * @param topicName topic name to create the node
   */
  abstract protected <K, V> void createNode(final String topicName);

  /**
   * update the zookeeper node for the topic partition each time it process the
   * messages.
   * 
   * @param maxTime   record timestamp to update
   * @param partition the partition the record and time belongs to
   * @return
   */
  abstract public void updateNode(final double maxTime, final int partition);

  /**
   * this function will retrieve the data from zookeeper path specified
   * 
   * @param zNode node name where the data needs to be pulled out.
   * @return byte array of data in the znode given
   */
  abstract protected byte[] getDataFromPath(final String zNode);

  /**
   * This is to find the minimum data in the each topic This method will be feeded
   * with a hashmap of topic-partion list with zero lag
   * 
   * @param zeroLag hashmap the contains all the topic partitions with zero lag
   * 
   * @return minimumTimestamp
   */
  abstract public double getMinimum(final HashMap<String, Long> zeroLag);

  /**
   * This is to find the minimum data in the each topic partition
   * 
   * @return minimumTimestamp
   */
  abstract public double getMinimum();

}