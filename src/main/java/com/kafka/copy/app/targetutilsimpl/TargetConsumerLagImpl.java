package com.kafka.copy.app.targetutilsimpl;

import static com.kafka.copy.app.constants.KafkaConstants.DEFAULT_GROUP_ID;
import static com.kafka.copy.app.constants.KafkaConstants.GROUPID_PREFIX;
import static com.kafka.copy.app.constants.KafkaConstants.CG_STATE_STABLE;
import static com.kafka.copy.app.constants.CommonConstants.INDEX_MAPPER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.kafkautilsimpl.KafkaConnection;
import com.kafka.copy.app.targetutils.TargetConsumerLag;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class TargetConsumerLagImpl<K, V> extends TargetConsumerLag<K, V> {
  private static final Logger logger = Logger.getLogger(TargetConsumerLagImpl.class.getName());

  private AdminClient client;
  private KafkaConfigReader configReader;
  private KafkaConsumer<K, V> consumer;
  private String inputTopicName;
  private List<String> outputTopicNameList;

  /**
   * @param inputTopicName
   * @param consumer
   * @param configReader
   * @param outputTopicNameList
   */
  public TargetConsumerLagImpl(final String inputTopicName, final KafkaConfigReader configReader,
      final String outputTopicNameList) {
    this.configReader = configReader;
    this.inputTopicName = inputTopicName;
    this.client = AdminClient.create(getAdminKafkaProperties());
    this.consumer = new KafkaConsumer<>(KafkaConnection.getTargetConsumerProperties(configReader));
    this.outputTopicNameList = Arrays.asList(outputTopicNameList.split(","));
  }

  @Override
  protected Properties getAdminKafkaProperties() {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configReader.getTargetServer());
    return props;
  }

  @Override
  protected Map<TopicPartition, OffsetAndMetadata> getOffsetMetadata(final String groupId) {

    Map<TopicPartition, OffsetAndMetadata> offsetMetaMap = new HashMap<TopicPartition, OffsetAndMetadata>();
    try {
      offsetMetaMap = this.client.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("error while getting offset metadata for group " + groupId + " with message " + e.getMessage());
    }
    return offsetMetaMap;
  }

  @Override
  protected Map<TopicPartition, Long> getPartitionEndOffsets(final Collection<TopicPartition> partitionInfo) {
    return this.consumer.endOffsets(partitionInfo);
  }

  @Override
  protected Map<String, KafkaFuture<ConsumerGroupDescription>> describeConsumerGroup(final String groupId) {
    return this.client.describeConsumerGroups(Arrays.asList(groupId)).describedGroups();
  }

  @Override
  protected Collection<ConsumerGroupListing> listConsumerGroups() {
    Collection<ConsumerGroupListing> offsetMetaMap = null;
    try {
      offsetMetaMap = this.client.listConsumerGroups().all().get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("error while listing all target consumer outside" + e.getMessage());
    }
    return offsetMetaMap;
  }

  @Override
  protected boolean checkAllTopicsInCosumerGroup(final String groupId) {
    final Set<String> consumingTopicSet = new HashSet<String>();
    getOffsetMetadata(groupId).keySet().forEach(topicPartition -> {
      consumingTopicSet.add(topicPartition.topic());
    });
    return consumingTopicSet.containsAll(this.outputTopicNameList) ? true : false;
  }

  @Override
  protected List<String> getStableConsumerGroups() {
    final List<String> consumerGroupList = new ArrayList<String>();
    listConsumerGroups().forEach(consumerGroupListing -> {
      if (checkAllTopicsInCosumerGroup(consumerGroupListing.groupId())) {
        describeConsumerGroup(consumerGroupListing.groupId()).values().forEach(consumerDesc -> {
          try {
            if (consumerDesc.get().state().toString().equals(CG_STATE_STABLE)) {
              consumerGroupList.add(consumerGroupListing.groupId());
            }
          } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }
        });
      }
    });
    return consumerGroupList;

  }

  @Override
  protected Map<String, HashMap<String, Long>> getConsumerTargetLag() {
    final List<String> activeConsumerGroupList = getStableConsumerGroups();
    final Map<String, HashMap<String, Long>> tgtConsumerLagMap = new HashMap<String, HashMap<String, Long>>();

    activeConsumerGroupList.forEach(groupId -> {
      final HashMap<String, Long> exceptLagMap = new HashMap<String, Long>();
      final Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets = getOffsetMetadata(groupId);
      final Map<TopicPartition, Long> topicEndOffsets = getPartitionEndOffsets(consumerGroupOffsets.keySet());

      consumerGroupOffsets.forEach((k, v) -> {
        final String topicName = k.topic();
        if (!topicName.equals(this.inputTopicName)) {
          long lag = topicEndOffsets.get(k) - v.offset();
          if (lag < 0) {
            lag = 0;
          }
          if (exceptLagMap.containsKey(topicName)) {
            long totalLag = exceptLagMap.get(topicName) + lag;
            exceptLagMap.put(topicName, totalLag);
          } else {
            exceptLagMap.put(topicName, lag);
          }
        }
      });

      tgtConsumerLagMap.put(groupId, exceptLagMap);
    });

    return tgtConsumerLagMap;
  }

  @Override
  public long getTargetActiveConsumerLag() {
    final Map<String, HashMap<String, Long>> groupIdList = getConsumerTargetLag();
    if (groupIdList.size() < 1) {
      return 0L;
    }
    final TreeSet<Long> versionSet = new TreeSet<Long>();
    groupIdList.keySet().forEach(key -> {
      String[] keySplitor = key.split(INDEX_MAPPER);
      versionSet.add(Long.parseLong(keySplitor[keySplitor.length - 1]));
    });
    final String consumerGroup = GROUPID_PREFIX + versionSet.last();
    final long maxLag = Collections.max(groupIdList.get(consumerGroup).values());
    return maxLag;
  }

  @Override
  @Deprecated
  protected String getStableTargetConsumer() {

    String consumerGroup = null;
    try {
      Iterator<ConsumerGroupListing> initialItr = this.client.listConsumerGroups().all().get().iterator();
      while (initialItr.hasNext()) {
        ConsumerGroupListing consumerGroupListing = initialItr.next();
        Iterator<Entry<TopicPartition, OffsetAndMetadata>> itr = this.client
            .listConsumerGroupOffsets(consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get().entrySet()
            .iterator();
        while (itr.hasNext()) {
          Entry<TopicPartition, OffsetAndMetadata> itrNext = itr.next();
          if (itrNext.getKey().topic().equals(this.inputTopicName)) {
            Iterator<Entry<String, KafkaFuture<ConsumerGroupDescription>>> itrInside = this.client
                .describeConsumerGroups(Arrays.asList(consumerGroupListing.groupId())).describedGroups().entrySet()
                .iterator();
            while (itrInside.hasNext()) {
              Entry<String, KafkaFuture<ConsumerGroupDescription>> describeConsumer = itrInside.next();
              if (describeConsumer.getValue().get().state().toString().equals("Stable")) {
                return consumerGroupListing.groupId();
              }
            }
          }
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    return consumerGroup == null ? DEFAULT_GROUP_ID : consumerGroup;
  }
}
