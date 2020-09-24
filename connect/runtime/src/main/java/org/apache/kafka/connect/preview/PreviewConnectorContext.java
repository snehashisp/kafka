package org.apache.kafka.connect.preview;

import java.util.Set;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;

public class PreviewConnectorContext implements ConnectorContext {

  private Set<Exception> exceptionSet;

  @Override
  public void requestTaskReconfiguration() {
    return;
  }

  @Override
  public void raiseError(Exception e) {
    exceptionSet.add(e);
  }

}
