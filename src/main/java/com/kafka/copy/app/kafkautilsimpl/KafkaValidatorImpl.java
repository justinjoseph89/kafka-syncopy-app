package com.kafka.copy.app.kafkautilsimpl;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import static com.kafka.copy.app.constants.CommonConstants.SPLIT_SEPERATOR;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.SRC_CLUSTER_NA;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.TGT_CLUSTER_NA;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.TOPIC_DESCRIBE_ERROR;
import static com.kafka.copy.app.constants.ExceptionMessageConstants.TGT_NO_TOPICS_FOUND;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.exceptions.SourceClusterNotAvailableException;
import com.kafka.copy.app.exceptions.TargetClusterNotAvailableException;
import com.kafka.copy.app.exceptions.TopicCreationException;
import com.kafka.copy.app.kafkautils.KafkaValidator;

import avro.shaded.com.google.common.collect.ImmutableList;

/**
 * @author justin
 *
 * @param <K>
 * @param <V>
 */
public class KafkaValidatorImpl<K, V> extends KafkaValidator {
  private static final Logger logger = Logger.getLogger(KafkaValidatorImpl.class.getName());
  private KafkaConfigReader configReader;
  private String inputTopicName;
  final KafkaConsumer<K, V> tgtConsumer;
  final KafkaConsumer<K, V> srcConsumer;

  /**
   * @param inputTopicName
   * @param consumer
   * @param configReader
   * @param outputTopicNameList
   */
  public KafkaValidatorImpl(final String inputTopicName, final KafkaConfigReader configReader) {
    this.configReader = configReader;
    this.inputTopicName = inputTopicName;
    this.tgtConsumer = new KafkaConsumer<K, V>(KafkaConnection.getTargetConsumerValidationProperties(configReader));
    this.srcConsumer = new KafkaConsumer<K, V>(KafkaConnection.getSourceConsumerValidationProperties(configReader));
  }

  @Override
  public boolean sourceTopicValidator() {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(
        KafkaConnection.getSourceConsumerValidationProperties(configReader));
    boolean chk = consumer.listTopics().keySet().containsAll(Arrays.asList(inputTopicName.split(SPLIT_SEPERATOR)));
    consumer.close();
    return chk;
  }

  @Override
  public boolean targetTopicValidator() {
    boolean chk = this.tgtConsumer.listTopics().keySet()
        .containsAll(Arrays.asList(inputTopicName.split(SPLIT_SEPERATOR)));
    if (!chk) {
      logger.warn(TGT_NO_TOPICS_FOUND);
      try {
        this.getAdminClient().describeTopics(Arrays.asList(inputTopicName.split(SPLIT_SEPERATOR))).all().get()
            .forEach((topic, future) -> {
              final int partitionNum = future.partitions().size();
              final int replicationFactor = future.partitions().get(0).replicas().size();
              this.createTopics(topic, partitionNum, replicationFactor);
              logger.info("Topic " + topic + " with partitionNum " + partitionNum + " and replicationFactor "
                  + replicationFactor + " is created in target cluster succesfully");
            });
      } catch (InterruptedException | ExecutionException e) {
        logger.error(TOPIC_DESCRIBE_ERROR);
        throw new TopicCreationException(TOPIC_DESCRIBE_ERROR, e);
      }

    }
    return this.tgtConsumer.listTopics().keySet().containsAll(Arrays.asList(inputTopicName.split(SPLIT_SEPERATOR)));
  }

  @Override
  protected void createTopics(final String topic, final int numPartitions, final int replicationFactor) {
    NewTopic topicConf = new NewTopic(topic, numPartitions, (short) replicationFactor);
    this.getAdminClient().createTopics(ImmutableList.of(topicConf));
  }

  @Override
  protected AdminClient getAdminClient() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.configReader.getSourceServer());
    return AdminClient.create(props);
  }

  @Override
  public void isSourceClusterAccessible() throws SourceClusterNotAvailableException {
    try {
      this.srcConsumer.listTopics();
    } catch (TimeoutException e) {
      logger.error(SRC_CLUSTER_NA);
      throw new SourceClusterNotAvailableException(SRC_CLUSTER_NA);
    }
  }

  @Override
  public void isTargetClusterAccessible() throws TargetClusterNotAvailableException {

    try {
      this.tgtConsumer.listTopics();
    } catch (TimeoutException e) {
      logger.error(TGT_CLUSTER_NA);
      throw new TargetClusterNotAvailableException(TGT_CLUSTER_NA);
    }

  }

  @Override
  public void close() {
    this.srcConsumer.close();
    this.tgtConsumer.close();
  }

}
