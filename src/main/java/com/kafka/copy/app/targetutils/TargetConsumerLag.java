package com.kafka.copy.app.targetutils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public abstract class TargetConsumerLag<K, V> {

  /**
   * @return
   */
  abstract protected Properties getAdminKafkaProperties();

  /**
   * @param groupId
   * @return
   */
  abstract protected Map<TopicPartition, OffsetAndMetadata> getOffsetMetadata(final String groupId);

  /**
   * @param partitionInfo
   * @return
   */
  abstract protected Map<TopicPartition, Long> getPartitionEndOffsets(final Collection<TopicPartition> partitionInfo);

  abstract protected Map<String, KafkaFuture<ConsumerGroupDescription>> describeConsumerGroup(final String groupId);

  abstract protected Collection<ConsumerGroupListing> listConsumerGroups();

  /**
   * @return Returns the any one consumer group that subscribed to the topic which
   *         is stable.
   * @return consumerGroupList processing the topic
   */

  @Deprecated
  abstract protected String getStableTargetConsumer();

  /**
   * This function will check whether the given stable group id is subscribed to
   * all the topics in output.
   * 
   * @return true or false based on that above condition.
   */

  abstract protected boolean checkAllTopicsInCosumerGroup(final String groupId);

  /**
   * This will return the list of all stable consumer groups present in the entire
   * cluster without checking any topics availability. This will become deprecated
   * in the coming versions as same functionality is available in version 2.6
   * 
   * @return list of all stable consumer groups in the target cluster
   */
  abstract protected List<String> getStableConsumerGroups();

  /**
   * This will read all active consumer groups that responsible for reading all
   * the topics present in the input topics.
   * 
   * This will calculate total lag across all partitions for the topic except the
   * one calling this method.
   * 
   * Use this method very carefully. Performance loss can happen with the raising
   * of consumer groups in the cluster
   * 
   * @return this will return a map of topics with total lag for each active
   *         consumer group.
   *
   */
  abstract protected Map<String, HashMap<String, Long>> getConsumerTargetLag();

  /**
   * This will find all the consumer groups with a map of lag for all the topics
   * (except the one that executing this method). find all the consumer groups
   * from the above map and iterate through and add the final version number to
   * the tree set to keep the natural ordering of version. from the treeset find
   * the last version since it is the one which is latest consumer group that is
   * reading the topics. once find the appropriate consumer group from all the
   * active consumer groups find the maximum lag for the topic which is present in
   * the consumer group listing This will check if any other topics in the input
   * is heading fast.
   *
   * @return return 0 if there is no consumer groups available for the input
   *         topics or it will return the maximum lag of any topic present in the
   *         consumer group except its own
   */
  abstract public long getTargetActiveConsumerLag();

}
