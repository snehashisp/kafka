package org.apache.kafka.connect.preview;

import java.util.Map;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

public class PreviewSourceTaskContext implements SourceTaskContext {

  private Map<String, String> taskConfigs;
  private OffsetStorageReader offsetStorageReader;

  public PreviewSourceTaskContext(Map<String, String> taskConfigs, OffsetStorageReader offsetStorageReader) {
    this.taskConfigs = taskConfigs;
    this.offsetStorageReader = offsetStorageReader;
  }

  @Override
  public Map<String, String> configs() {
    return taskConfigs;
  }

  @Override
  public OffsetStorageReader offsetStorageReader() {
    return offsetStorageReader;
  }
}
