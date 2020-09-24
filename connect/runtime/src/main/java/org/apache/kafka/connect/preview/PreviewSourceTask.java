package org.apache.kafka.connect.preview;

import java.util.List;
import java.util.Map;
import jdk.tools.jlink.resources.plugins;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;

public class PreviewSourceTask implements Runnable {

  private Logger log;
  private Callback<PreviewMetadataResponse> responseCallback;
  private Plugins plugins;
  private OffsetStorageReader offsetStorageReader;
  private Map<String, String> connectorConfigs;
  private int taskId;
  private List<String> previewErrors;
  private ClassLoader savedLoader;
  private PreviewMetadataResponse PreviewMetadataResponse;
  private String connectorName;

  public PreviewSourceTask(Callback<PreviewMetadataResponse> responseCallback, String connectorName,
      Map<String, String> connectorConfigs, Plugins plugins,
      OffsetStorageReader offsetStorageReader, int taskId) {
    this.connectorConfigs = connectorConfigs;
    this.plugins = plugins;
    this.offsetStorageReader = offsetStorageReader;
    this.savedLoader = plugins.currentThreadLoader();
    this.taskId = taskId;
    this.connectorName = connectorName;
    log = new LogContext("[Preview " + connectorName + "]").logger(PreviewSourceTask.class);
  }

  public PreviewSourceTask(Callback<PreviewMetadataResponse> responseCallback, String connectorName,
      Map<String, String> connectorConfigs, Plugins plugins,
      OffsetStorageReader offsetStorageReader) {
    this(responseCallback, connectorName, connectorConfigs, plugins, offsetStorageReader, 0);
  }

  private Connector initConnector() throws PreviewInitConnectorExceptions {
    log.info("Init Preview Connector");
    String connectorClass = connectorConfigs.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
    ClassLoader connectorClassLoader = plugins.delegatingLoader().connectorLoader(connectorClass);
    savedLoader = plugins.compareAndSwapLoaders(connectorClassLoader);
    Connector connector = plugins.newConnector(connectorClass);
    connector.initialize(new PreviewConnectorContext());
    return connector;
  }

  private Task initTask(Connector connector) throws PreviewInitTaskException {
    log.info("Init preview task");
    Class taskClass = connector.taskClass();
    Task task = plugins.newTask(taskClass);
    return task;
  }

  @Override
  public void run() {
    try {
      Connector connector = initConnector();
      Task connectorTask = initTask(connector);
      List<Map<String, String>> taskConfigs = connector
          .taskConfigs(Integer.getInteger(connectorConfigs.get(ConnectorConfig.TASKS_MAX_CONFIG)));
      PreviewMetadataResponse = new PreviewMetadataResponse(ConnectorConfig.CONNECTOR_CLASS_CONFIG,
          taskConfigs.get(taskId), taskId);
    } catch (Exception e) {
      previewErrors.add(e.getMessage());
      e.printStackTrace();
    } finally {
      plugins.compareAndSwapLoaders(savedLoader);
      responseCallback.onCompletion(null, PreviewMetadataResponse);
    }
  }

  class PreviewInitConnectorExceptions extends Exception {

    PreviewInitConnectorExceptions() {
      super("An error occured while loading connector for preview");
    }
  }

  class PreviewInitTaskException extends Exception {

    PreviewInitTaskException(String message) {
      super("An error occured while loading task");
    }
  }
}
