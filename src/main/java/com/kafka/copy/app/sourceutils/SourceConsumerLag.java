package com.kafka.copy.app.sourceutils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public abstract class SourceConsumerLag<K, V> {

  /**
   * return the kafka properties for creating admin module
   * 
   * @return Properties for kafka admin client creation
   */
  abstract protected Properties adminKafkaProperties();

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

  /**
   * this function is to find the lag for the partition which is executing this
   * partition.
   * 
   * @param groupId        of the topic which consumes.
   * @param inputTopicName topic that records belongs to.
   * @param rec            consumer record from kafka topic.
   * @return this will return the long value of lag implicated to this partition
   *         where record belongs
   */
  abstract public long getSourcePartitionConsumerLag(final String groupId, final String inputTopicName,
      final ConsumerRecord<K, V> rec);

  /**
   * this function return all the partitions of a group id with zero lag value (It
   * can be either already caught up or not yet started consuming data) Also this
   * will return all partitions size for this consumer group. Will this return
   * every input topics details) CRITICAL:CHANGE IT
   * 
   * @return map of all topic-partition with its lag (if zero) also total number
   *         of elements in the topic
   */
  abstract public HashMap<String, Long> getTopicPartitionWithoutLag(final String groupId);

}
