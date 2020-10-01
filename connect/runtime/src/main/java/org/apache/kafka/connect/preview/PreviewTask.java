package org.apache.kafka.connect.preview;

public interface PreviewTask extends Runnable {

  public PreviewResponse getResponse();
  public void stopPreview();
}
