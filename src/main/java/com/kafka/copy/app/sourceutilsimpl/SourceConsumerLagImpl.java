package com.kafka.copy.app.sourceutilsimpl;

import static com.kafka.copy.app.constants.CommonConstants.INDEX_MAPPER;
import static com.kafka.copy.app.constants.CommonConstants.ELEMENT_SIZE;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.sourceutils.SourceConsumerLag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class SourceConsumerLagImpl<K, V> extends SourceConsumerLag<K, V> {
  private KafkaConsumer<K, V> consumer;
  private AdminClient client;
  private KafkaConfigReader configReader;

  /**
   * @param consumer
   * @param configReader
   */
  public SourceConsumerLagImpl(final KafkaConsumer<K, V> consumer, final KafkaConfigReader configReader) {
    this.consumer = consumer;
    this.configReader = configReader;
    this.client = AdminClient.create(adminKafkaProperties());
  }

  @Override
  protected Properties adminKafkaProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configReader.getSourceServer());
    return props;
  }

  @Override
  protected Map<TopicPartition, OffsetAndMetadata> getOffsetMetadata(final String groupId) {

    Map<TopicPartition, OffsetAndMetadata> offsetMetaMap = new HashMap<TopicPartition, OffsetAndMetadata>();
    try {
      offsetMetaMap = this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return offsetMetaMap;
  }

  @Override
  protected Map<TopicPartition, Long> getPartitionEndOffsets(final Collection<TopicPartition> partitionInfo) {
    return this.consumer.endOffsets(partitionInfo);
  }

 
  @Override
  public long getSourcePartitionConsumerLag(final String groupId, final String inputTopicName,
      final ConsumerRecord<K, V> rec) {

    final HashMap<String, Long> partitionLagMap = new HashMap<String, Long>();
    final String partitionIndexName = groupId.concat(INDEX_MAPPER).concat(inputTopicName).concat(INDEX_MAPPER)
        + rec.partition();

    final Map<TopicPartition, OffsetAndMetadata> sourceConsumerOffsets = getOffsetMetadata(groupId);
    final Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(sourceConsumerOffsets.keySet());
    sourceConsumerOffsets.entrySet().iterator().forEachRemaining(partitionData -> {
      long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
      if (lag < 0) {
        lag = 0;
      }
      partitionLagMap.put(
          groupId.concat(INDEX_MAPPER).concat(inputTopicName).concat(INDEX_MAPPER) + partitionData.getKey().partition(),
          lag);
    });

    return partitionLagMap.containsKey(partitionIndexName) ? partitionLagMap.get(partitionIndexName) : 0L;
  }

  @Override
  public HashMap<String, Long> getTopicPartitionWithoutLag(final String groupId) {

    final HashMap<String, Long> zeroLagMap = new HashMap<String, Long>();
    final List<String> parititionListSize = new ArrayList<String>();
    final Map<TopicPartition, OffsetAndMetadata> sourceConsumerOffsets = getOffsetMetadata(groupId);
    final Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(sourceConsumerOffsets.keySet());

    sourceConsumerOffsets.entrySet().forEach(partitionData -> {
      long lag = topicEndOffsets.get(partitionData.getKey()) - partitionData.getValue().offset();
      if (lag <= 0) {
        lag = 0;
        zeroLagMap.put(partitionData.getKey().topic().concat(INDEX_MAPPER) + partitionData.getKey().partition(), lag);
      }
      parititionListSize.add(partitionData.getKey().topic().concat(INDEX_MAPPER) + partitionData.getKey().partition());
    });
    zeroLagMap.put(ELEMENT_SIZE, Long.valueOf(parititionListSize.size()));

    return zeroLagMap;
  }
}
