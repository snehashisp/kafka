package org.apache.kafka.connect.preview;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.Callback;

public class PreviewResponse {

  private String connectorName;
  private Map<String, String> taskConfig;
  private int taskId;
  private List<String> errors;
  private List<Object> records;
  private int requstedRecords;
  private Callback<?> responseCallback;

  public PreviewResponse(String connectorName, int taskId, Callback<?> responseCallback, int requstedRecords) {
    this.responseCallback = responseCallback;
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.errors = Collections.synchronizedList(new ArrayList<>());
    this.records = Collections.synchronizedList(new ArrayList<>());
    this.requstedRecords = requstedRecords;
  }

  public PreviewResponse(String connectorName, int taskId, Callback<?> responseCallback) {
    this(connectorName, taskId, responseCallback, 1);
  }

  public void addTaskConfigs(Map<String, String> taskConfig) {
    this.taskConfig = taskConfig;
  }

  public void addErrors(Exception e) {
    errors.add(e.getMessage());
    responseCallback.onCompletion(null, null);
  }

  public void addErrors(String error) {
    errors.add(error);
    responseCallback.onCompletion(null, null);
  }

  public void addRecord(ConnectRecord<SourceRecord> record) {
    synchronized (records) {
      records.add(new ConnectRecordResponse<SourceRecord>(record));
      if (records.size() >= requstedRecords) {
        responseCallback.onCompletion(null, null);
      }
    }
  }

  public void addRecord(Object record) {
    synchronized (records) {
      records.add(record);
      if (records.size() >= requstedRecords) {
        responseCallback.onCompletion(null, null);
      }
    }
  }

  public void signalFinished() {
    responseCallback.onCompletion(null, null);
  }

  @JsonProperty
  public List<Object> getRecords() {
    synchronized (records) {
      return records.subList(0, Math.min(records.size(), requstedRecords));
    }
  }

  @JsonProperty
  public Map<String, String> getTaskConfig() {
    return taskConfig;
  }

  @JsonProperty
  public String getConnectorName() {
    return connectorName;
  }

  @JsonProperty
  public int getTaskId() {
    return taskId;
  }

  @JsonProperty
  public List<String> getErrors() { return errors; }

  class ConnectRecordResponse<R extends ConnectRecord<R>> {
    private ConnectRecord<R> connectRecord;
    public ConnectRecordResponse(ConnectRecord<R> connectRecord) {
      this.connectRecord = connectRecord;
    }
    @JsonProperty
    public String getKey() {
      return connectRecord.key() == null ? null : connectRecord.key().toString();
    }
    @JsonProperty
    public String getHeaders() {
      return connectRecord.headers() == null ? null : connectRecord.headers().toString();
    }
    @JsonProperty
    public String getValue() {
      return connectRecord.value() == null ? null : connectRecord.value().toString();
    }
    @JsonProperty
    public String getTopic() {
      return connectRecord.topic();
    }
  }
}
