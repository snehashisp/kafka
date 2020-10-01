package org.apache.kafka.connect.preview;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;

public class PreviewConnectorTask implements PreviewTask {

  private final Logger log;
  private final Plugins plugins;
  private final MemoryOffsetBackingStore memoryOffsetBackingStore;
  private final OffsetStorageReader offsetStorageReader;
  private final OffsetStorageWriter offsetStorageWriter;
  private final Map<String, String> originalConfigs;
  private final int taskId;
  private ClassLoader savedLoader;
  private PreviewResponse previewResponse;
  private final String connectorName;
  private AtomicBoolean allowTaskExecution;
  private Connector connector = null;
  private Task task = null;
  private boolean taskStarted = false, connectorStarted = false;
  private RetryWithToleranceOperator retryWithToleranceOperator;
  private TransformationChain<SourceRecord> sourceRecordTransformationChain;
  private TransformationChain<SinkRecord> sinkRecordTransformationChain;
  private Worker worker;
  private int records;

  private Converter headerConverter, keyConverter, valueConverter;

  public PreviewConnectorTask(
      Callback<?> responseCallback,
      String connectorName,
      Map<String, String> connectorConfigs,
      Worker worker,
      int records,
      int taskId) {
    this.originalConfigs = connectorConfigs;
    this.plugins = worker.getPlugins();
    this.savedLoader = plugins.currentThreadLoader();
    this.taskId = taskId;
    this.connectorName = connectorName;
    this.records = records;
    this.previewResponse = new PreviewResponse(connectorName, taskId, responseCallback, records);
    this.allowTaskExecution = new AtomicBoolean(true);
    log = new LogContext("[Preview " + connectorName + "]").logger(PreviewConnectorTask.class);

    memoryOffsetBackingStore = new MemoryOffsetBackingStore();
    this.offsetStorageWriter = new OffsetStorageWriter(
        memoryOffsetBackingStore,
        connectorName,
        worker.getInternalKeyConverter(),
        worker.getInternalValueConverter()
    );
    this.offsetStorageReader = new OffsetStorageReaderImpl(
        memoryOffsetBackingStore,
        connectorName,
        worker.getInternalKeyConverter(),
        worker.getInternalValueConverter()
    );
    this.worker = worker;
  }

  public PreviewConnectorTask(Callback<?> responseCallback,
      String connectorName,
      Map<String, String> connectorConfigs,
      Worker worker) {
    this(responseCallback, connectorName, connectorConfigs, worker, 10, 0);
  }

  public PreviewConnectorTask(Callback<?> responseCallback,
      String connectorName,
      Map<String, String> connectorConfigs,
      Worker worker,
      int records) {
    this(responseCallback, connectorName, connectorConfigs, worker, records, 0);
  }

  private void initConverters(ConnectorConfig connectorConfig) {
    keyConverter = plugins.newConverter(connectorConfig, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER);
    keyConverter = keyConverter == null ? plugins.newConverter(
        worker.getConfig(),
        WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER
    ) : keyConverter;
    valueConverter = plugins.newConverter(
        connectorConfig,
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER
    );
    valueConverter = valueConverter == null ? plugins.newConverter(
        worker.getConfig(),
        WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER
    ) : valueConverter;
    headerConverter = plugins.newConverter(
        connectorConfig,
        WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER
    );
    headerConverter = headerConverter == null ? plugins.newConverter(
        worker.getConfig(),
        WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
        ClassLoaderUsage.CURRENT_CLASSLOADER
    ) : headerConverter;
  }

  private Connector initConnector(ConnectorConfig connectorConfigs) {
    log.info("Initializing preview Connector");
    String connectorClass = connectorConfigs.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
    ClassLoader connectorClassLoader = plugins.delegatingLoader().connectorLoader(connectorClass);
    savedLoader = Plugins.compareAndSwapLoaders(connectorClassLoader);
    Connector connector = plugins.newConnector(connectorClass);
    connector.initialize(new PreviewConnectorContext(previewResponse));
    return connector;
  }


  private Task initTask() {
    log.info("Initializing preview task");
    Class<? extends Task> taskClass = connector.taskClass();
    Task task = plugins.newTask(taskClass);
    return task;
  }

  private Headers convertHeadersFor(ConsumerRecord<byte[], byte[]> record) {
    Headers result = new ConnectHeaders();
    org.apache.kafka.common.header.Headers recordHeaders = record.headers();
    if (recordHeaders != null) {
      String topic = record.topic();
      for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
        SchemaAndValue schemaAndValue = ((HeaderConverter) headerConverter)
            .toConnectHeader(topic, recordHeader.key(), recordHeader.value());
        result.add(recordHeader.key(), schemaAndValue);
      }
    }
    return result;
  }

  private SinkRecord convertAndTransformRecord(final ConsumerRecord<byte[], byte[]> msg) {
    SchemaAndValue keyAndSchema = retryWithToleranceOperator
        .execute(() -> keyConverter.toConnectData(msg.topic(), msg.headers(), msg.key()),
            Stage.KEY_CONVERTER, keyConverter.getClass());

    SchemaAndValue valueAndSchema = retryWithToleranceOperator
        .execute(() -> valueConverter.toConnectData(msg.topic(), msg.headers(), msg.value()),
            Stage.VALUE_CONVERTER, valueConverter.getClass());

    Headers headers = retryWithToleranceOperator
        .execute(() -> convertHeadersFor(msg), Stage.HEADER_CONVERTER, headerConverter.getClass());

    if (retryWithToleranceOperator.failed()) {
      return null;
    }
    Long timestamp = ConnectUtils.checkAndConvertTimestamp(msg.timestamp());
    SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
        keyAndSchema.schema(), keyAndSchema.value(),
        valueAndSchema.schema(), valueAndSchema.value(),
        msg.offset(),
        timestamp,
        msg.timestampType(),
        headers);
    return sinkRecordTransformationChain.apply(origRecord);
  }

  private void runSourceTask() throws Exception {
    log.info("Starting Source Task");
    try {
      SourceTask sourceTask = (SourceTask) task;
      while (allowTaskExecution.get()) {
        List<SourceRecord> records = sourceTask.poll();
        if (records != null) {
          for (SourceRecord preTransformedRecord : records) {
            retryWithToleranceOperator.sourceRecord(preTransformedRecord);
            final ConnectRecord<SourceRecord> connectRecord =
                sourceRecordTransformationChain.apply(preTransformedRecord);
            previewResponse.addRecord(connectRecord);
            offsetStorageWriter.offset(preTransformedRecord.sourcePartition(),
                preTransformedRecord.sourceOffset());
          }
        }
      }
    } catch (Exception e) {
      previewResponse.addErrors(e);
      e.printStackTrace();
    } finally {
      sourceRecordTransformationChain.close();
    }
  }

  private void runSinkTask(
      KafkaConsumer<byte[], byte[]> consumer,
      PreviewSinkTaskContext context
  ) throws Exception {
    log.info("Starting Sink task");
    try {
      SinkTask sinkTask = (SinkTask) task;
      List<SinkRecord> msgBatch = new ArrayList<>();
      int totalMessages = 0;
      while (totalMessages < records && allowTaskExecution.get()) {
        long timeout = Math.max(0, context.getTimeout());
        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(Duration.ofMillis(timeout));
        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
          retryWithToleranceOperator.consumerRecord(msg);
          SinkRecord transRecord = convertAndTransformRecord(msg);
          log.info(transRecord.toString());
          if(transRecord != null) {
            msgBatch.add(transRecord);
            totalMessages += 1;
          }
          if(totalMessages >= records)
            break;
        }
        sinkTask.put(msgBatch);
        msgBatch.clear();
      }
    } catch (Exception e) {
      previewResponse.addErrors(e);
      e.printStackTrace();
    } finally {
      sinkRecordTransformationChain.close();
      consumer.close();
    }
  }

  private boolean validate() {
    Config configs = connector.validate(originalConfigs);
    String errorMessages = new String("");
    for (ConfigValue value : configs.configValues()) {
      if (!value.errorMessages().isEmpty()) {
        errorMessages += value.errorMessages() + "\n";
      }
    }
    if (!errorMessages.equals("")) {
      previewResponse.addErrors(errorMessages);
      return false;
    }
    return true;
  }

  @Override
  public void run() {
    try {
      ConnectorConfig connectorConfigs = new ConnectorConfig(plugins, originalConfigs);
      connector = initConnector(connectorConfigs);
      if (!validate()) {
        return;
      }
      ;
      if (!connector.isPreviewAllowed()) {
        previewResponse.addErrors("Preview not allowed for connector. Its for your own good :)");
        return;
      }
      retryWithToleranceOperator = new RetryWithToleranceOperator(
          connectorConfigs.errorRetryTimeout(), connectorConfigs.errorMaxDelayInMillis(),
          connectorConfigs.errorToleranceType(),
          Time.SYSTEM);

      connector.start(connectorConfigs.originalsStrings());
      connectorStarted = true;

      if (previewResponse.getErrors().size() == 0) {

        Map<String, String> taskConfig = connector
            .taskConfigs(connectorConfigs.getInt(ConnectorConfig.TASKS_MAX_CONFIG).intValue())
            .get(taskId);

        previewResponse.addTaskConfigs(taskConfig);
        task = initTask();
        if (SourceConnector.class.isAssignableFrom(connector.getClass())) {

          this.sourceRecordTransformationChain = new TransformationChain<>(
              connectorConfigs.<SourceRecord>transformations(),
              retryWithToleranceOperator
          );

          ((SourceTask) task)
              .initialize(new PreviewSourceTaskContext(taskConfig, offsetStorageReader));
          memoryOffsetBackingStore.start();
          task.start(taskConfig);
          taskStarted = true;
          runSourceTask();
        } else {
          SinkConnectorConfig.validate(taskConfig);
          KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
              Worker.consumerConfigs(
                  new ConnectorTaskId(connectorName + "preview", taskId),
                  worker.getConfig(),
                  connectorConfigs,
                  plugins.connectorClass(
                      connectorConfigs.getString(ConnectorConfig.CONNECTOR_CLASS_CONFIG)
                  ),
                  worker.getConnectorClientConfigOverridePolicy()
              )
          );
          if (SinkConnectorConfig.hasTopicsConfig(taskConfig)) {
            consumer.subscribe(Arrays.asList(taskConfig.get(SinkTask.TOPICS_CONFIG).split(",")));
          } else {
            consumer.subscribe(Pattern.compile(taskConfig.get(SinkTask.TOPICS_REGEX_CONFIG)));
          }
          consumer.seekToBeginning(consumer.assignment());
          initConverters(connectorConfigs);
          this.sinkRecordTransformationChain = new TransformationChain<>(
              connectorConfigs.<SinkRecord>transformations(),
              retryWithToleranceOperator
          );
          PreviewSinkTaskContext sinkTaskContext =
              new PreviewSinkTaskContext(taskConfig, consumer, previewResponse);
          ((SinkTask) task)
              .initialize(sinkTaskContext);
          task.start(taskConfig);
          taskStarted = true;
          runSinkTask(consumer, sinkTaskContext);
        }
      }
    } catch (Exception e) {
      previewResponse.addErrors(e);
      e.printStackTrace();
    } finally {
      log.info("Exiting preview");
      memoryOffsetBackingStore.stop();
      Plugins.compareAndSwapLoaders(savedLoader);
      previewResponse.signalFinished();
    }
  }

  @Override
  public void stopPreview() {
    if (allowTaskExecution.get()) {
      if (taskStarted) {
        task.stop();
      }
      if (connectorStarted) {
        connector.stop();
      }
      allowTaskExecution.set(false);
    }
    previewResponse.signalFinished();
  }

  @Override
  public PreviewResponse getResponse() {
    return previewResponse;
  }

}
