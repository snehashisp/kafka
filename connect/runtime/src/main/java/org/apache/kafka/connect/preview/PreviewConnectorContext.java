package org.apache.kafka.connect.preview;

import java.util.Set;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;

public class PreviewConnectorContext implements ConnectorContext {

  private PreviewResponse previewResponse;

  public PreviewConnectorContext(PreviewResponse previewResponse) {
    this.previewResponse = previewResponse;
  }

  @Override
  public void requestTaskReconfiguration() {
    return;
  }

  @Override
  public void raiseError(Exception e){
    previewResponse.addErrors(e);
  }

  @Override
  public boolean previewEnabled() {
    return true;
  }

}
