package org.apache.kafka.connect.preview;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class PreviewMetadataResponse {

  private String connectorClass;
  private Map<String, String> taskConfig;
  private int taskId;

  public PreviewMetadataResponse(String connectorClass, Map<String, String> taskConfig, int taskId) {
    this.connectorClass = connectorClass;
    this.taskConfig = taskConfig;
    this.taskId = taskId;
  }

  @JsonProperty
  public Map<String, String> getTaskConfig() {
    return taskConfig;
  }

  @JsonProperty
  public String getConnectorClass() {
    return connectorClass;
  }
  @JsonProperty
  public int getTaskId() {
    return taskId;
  }

}
