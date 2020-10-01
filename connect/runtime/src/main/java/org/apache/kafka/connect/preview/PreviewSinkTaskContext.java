package org.apache.kafka.connect.preview;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.types.Field.Array;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class PreviewSinkTaskContext implements SinkTaskContext {

  private Map<String, String> taskConfigs;
  private Map<TopicPartition, Long> offsets;
  private KafkaConsumer<byte[], byte[]> consumer;
  private long timeout;
  private Set<TopicPartition> pausedPartitions;
  private PreviewResponse previewResponse;

  public PreviewSinkTaskContext(
      Map<String, String> taskConfigs,
      KafkaConsumer<byte[], byte[]> consumer,
      PreviewResponse previewResponse
  ) {
    this.taskConfigs = taskConfigs;
    this.consumer = consumer;
    this.timeout = -1L;
    this.pausedPartitions = new HashSet<>();
    this.previewResponse = previewResponse;
  }

  @Override
  public Map<String, String> configs() {
    return taskConfigs;
  }

  @Override
  public void offset(Map<TopicPartition, Long> offsets) {
    for(Entry<TopicPartition, Long> tpOffsets : offsets.entrySet()) {
      consumer.seek(tpOffsets.getKey(), tpOffsets.getValue());
    }
  }

  @Override
  public void offset(TopicPartition tp, long offset) {
    consumer.seek(tp, offset);
  }

  @Override
  public void timeout(long timeoutMs) {
    this.timeout = timeoutMs;
  }

  public Map<TopicPartition, Long> getOffsets() {
    return offsets;
  }

  public long getTimeout() {
    return timeout;
  }


  @Override
  public Set<TopicPartition> assignment() {
    if (consumer == null) {
      throw new IllegalWorkerStateException("SinkTaskContext may not be used to look up partition assignment until the task is initialized");
    }
    return consumer.assignment();
  }

  @Override
  public void pause(TopicPartition... partitions) {
    if (consumer == null) {
      throw new IllegalWorkerStateException("SinkTaskContext may not be used to pause consumption until the task is initialized");
    }
    try {
      Collections.addAll(pausedPartitions, partitions);
      consumer.pause(Arrays.asList(partitions));
    } catch (IllegalStateException e) {
      throw new IllegalWorkerStateException("SinkTasks may not pause partitions that are not currently assigned to them.", e);
    }
  }

  @Override
  public void resume(TopicPartition... partitions) {
    if (consumer == null) {
      throw new IllegalWorkerStateException("SinkTaskContext may not be used to pause consumption until the task is initialized");
    }
    try {
      pausedPartitions.removeAll(Arrays.asList(partitions));
      consumer.resume(Arrays.asList(partitions));
    } catch (IllegalStateException e) {
      throw new IllegalWorkerStateException("SinkTasks may not pause partitions that are not currently assigned to them.", e);
    }
  }

  @Override
  public void requestCommit() {
    return;
  }


  @Override
  public void previewOutputs(List<Object> outputs) {
    for(Object output: outputs) {
      previewResponse.addRecord(output);
    }
  }

  @Override
  public boolean previewEnabled() {
    return true;
  }
}
