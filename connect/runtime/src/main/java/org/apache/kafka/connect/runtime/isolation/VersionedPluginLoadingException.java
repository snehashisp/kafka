package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;

public class VersionedPluginLoadingException extends ConnectException {

    private List<String> availableVersions = null;

    public VersionedPluginLoadingException(String message) {
        super(message);
    }

    public VersionedPluginLoadingException(String message, List<String> availableVersions) {
        super(message);
        this.availableVersions = availableVersions;
    }

    public VersionedPluginLoadingException(String message, Throwable cause) {
        super(message, cause);
    }

    public VersionedPluginLoadingException(String message, Throwable cause, List<String> availableVersions) {
        super(message, cause);
        this.availableVersions = availableVersions;
    }

    public List<String> availableVersions() {
        return availableVersions;
    }
}
