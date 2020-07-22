package com.kafka.copy.app.processors;

import java.time.Duration;

import static com.kafka.copy.app.constants.KafkaConstants.GROUPID_PREFIX;
import static com.kafka.copy.app.constants.CommonConstants.SPLIT_SEPERATOR;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.copy.app.configreader.KafkaConfigReader;
import com.kafka.copy.app.datautils.FieldSelector;
import com.kafka.copy.app.datautilsimpl.FieldSelectorImpl;
import com.kafka.copy.app.kafkautilsimpl.KafkaConnection;
import com.kafka.copy.app.sourceutils.SourceConsumerLag;
import com.kafka.copy.app.sourceutilsimpl.SourceConsumerLagImpl;
import com.kafka.copy.app.targetutils.TargetConsumerLag;
import com.kafka.copy.app.targetutilsimpl.TargetConsumerLagImpl;
import com.kafka.copy.app.zkutils.ZkConnect;
import com.kafka.copy.app.zkutilsimpl.ZkConnectImpl;

/**
 * @author justin
 *
 */
public class KafkaCopyAppProcessor<K, V> {
  private double maxTime = 0d;
  private double leadTime = 0d; // Initial Assignment
  private String inputTopicName;
  private String outputTopicName;
  private String groupId;
  private KafkaConfigReader configReader;
  private FieldSelector fieldSelector;
  private String outputTopicNameList;
  private boolean isZkNodeUpd;

  public KafkaCopyAppProcessor(final String inputTopicName, final KafkaConfigReader configReader,
      final String inputTopicNameList) {
    this.inputTopicName = inputTopicName;
    this.outputTopicName = inputTopicName;
    this.configReader = configReader;
    this.groupId = GROUPID_PREFIX + this.configReader.getAppVersion();
    this.fieldSelector = new FieldSelectorImpl();
    this.outputTopicNameList = inputTopicNameList;
    this.isZkNodeUpd = this.configReader.isZkNodeUpd();
  }

  /**
   * this has the starting point of each threads to execute
   */
  public void start() {
    final ZkConnect zk = new ZkConnectImpl(this.inputTopicName, isZkNodeUpd, this.configReader);
    final TargetConsumerLag<K, V> targetLag = new TargetConsumerLagImpl<K, V>(this.inputTopicName, this.configReader,
        this.outputTopicNameList);

    final KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(
        KafkaConnection.getSourceConsumerProperties(this.groupId, this.configReader));

    final SourceConsumerLag<K, V> consumerLag = new SourceConsumerLagImpl<K, V>(consumer, this.configReader);

    final KafkaProducer<K, V> producer = new KafkaProducer<K, V>(
        KafkaConnection.getSourceProducerProperties(this.configReader));

    consumer.subscribe(java.util.Arrays.asList(this.inputTopicName));

    final String[] fieldNameToConsider = this.configReader.getTopicFields().get(this.inputTopicName)
        .split(SPLIT_SEPERATOR);

    boolean considerLag = false;

    while (true) {
      final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));

      for (ConsumerRecord<K, V> rec : records) {
        boolean isNotConsumed = messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider,
            targetLag);
        while (isNotConsumed) {
          isNotConsumed = messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider,
              targetLag);
        }
        consumer.commitSync();
      }

      considerLag = true;

    }
  }

  /**
   * this function contains the entire process flow of this consumer. there can be
   * some performance implication for sure since we are trying to make this
   * synchronized
   * 
   * @param rec                 consumer record from the topic
   * @param producer            producer object to start producing the data to
   *                            output topic
   * @param consumerLag         consumerlag class object to identify the lag and
   *                            other utils from the source consumer side.
   * @param considerLag         this is something that has been added to handle to
   *                            the first ever message that is going to process
   * @param zk                  zookeeper connection object to make the access to
   *                            the zookeeper
   * @param fieldNameToConsider this should be a comma seperated field position
   *                            that contains the field that needs to be
   *                            considered
   * @param targetLag           this is target consumer lag object to handle the
   *                            target consumer details
   * @return
   */
  private boolean messageSendRecursive(final ConsumerRecord<K, V> rec, final KafkaProducer<K, V> producer,
      final SourceConsumerLag<K, V> consumerLag, final boolean considerLag, final ZkConnect zk,
      final String[] fieldNameToConsider, final TargetConsumerLag<K, V> targetLag) {

    // get the timestamp of the data based on the key provided
    final long recTimestamp = this.fieldSelector.getTimestampFromData(rec, fieldNameToConsider);
    // get the timestamp of the data based on the key provided
    final long lag = consumerLag.getSourcePartitionConsumerLag(groupId, inputTopicName, rec);
    final long maxLag = targetLag.getTargetActiveConsumerLag();

    if (maxTime == 0d) {
      maxTime = zk.getMinimum() == 0d ? recTimestamp : zk.getMinimum();
      maxTime = maxTime + this.configReader.getDeltaValue();
      zk.updateNode(maxTime, rec.partition());
    }
    if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
      if (lag == 0 && considerLag == true) {
        this.leadTime = 0;
      } else {
        this.leadTime = recTimestamp - maxTime;
      }

      if (this.leadTime <= this.configReader.getSmallDeltaValue()) {
        producer.send(new ProducerRecord<K, V>(this.outputTopicName, rec.partition(), rec.key(), rec.value()));
      }
    }

    if (this.leadTime > this.configReader.getSmallDeltaValue()) {
      zk.updateNode(maxTime, rec.partition());
      while (zk.getMinimum(consumerLag.getTopicPartitionWithoutLag(this.groupId)) < maxTime
          || maxLag > this.configReader.getSmallDeltaValue()) {
        try {
          Thread.sleep(this.configReader.getSleepTimeMs());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      maxTime = maxTime + this.configReader.getDeltaValue();
      this.leadTime = 0;
      messageSendRecursive(rec, producer, consumerLag, considerLag, zk, fieldNameToConsider, targetLag);
      return true;
    }

    return false;

  }

}
