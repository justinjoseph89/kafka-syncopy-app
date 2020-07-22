package com.kafka.copy.app.kafkautils;

import org.apache.kafka.clients.admin.AdminClient;

import com.kafka.copy.app.exceptions.SourceClusterNotAvailableException;
import com.kafka.copy.app.exceptions.TargetClusterNotAvailableException;

/**
 * @author justin
 *
 */
public abstract class KafkaValidator {
  /**
   * this function will validate whether the topics are present in the source
   * cluster. if not it will throw be out of the application.
   */

  abstract public boolean sourceTopicValidator();

  /**
   * this function will validate whether the topics are present in the target
   * cluster. if not it will identify the basic configurations of the source
   * cluster topics and create it accordingly
   */

  abstract public boolean targetTopicValidator();

  /**
   * this function will create topics in the target cluster based on the
   * properties it identified from the source cluster or the information it
   * provided
   * 
   * @param topic             topic name to be created in the target
   * @param numPartitions     number of partitions to be created for the target
   *                          topic
   * @param replicationFactor replication factor to be created for the target
   *                          topic
   */
  abstract protected void createTopics(String topic, int partitionNum, int replicationFactor);

  abstract protected AdminClient getAdminClient();

  abstract public void isSourceClusterAccessible() throws SourceClusterNotAvailableException;

  abstract public void isTargetClusterAccessible() throws TargetClusterNotAvailableException;
  abstract public void close() ;


}
