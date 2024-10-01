package org.apache.kafka.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PluginVersionUtils {

    public static VersionRange connectorVersionRequirement(String version) throws InvalidVersionSpecificationException {
        if (version == null || version.equals("latest")) {
            return null;
        }
        version = version.trim();

        // check first if the given version is valid
        VersionRange.createFromVersionSpec(version);

        // now if the version is not enclosed we consider it as a hard requirement and enclose it in []
        if (!version.startsWith("[") && !version.startsWith("(")) {
            version = "[" + version + "]";
        }
        return VersionRange.createFromVersionSpec(version);
    }

    public static class PluginVersionValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {

            try {
                connectorVersionRequirement((String) value);
            } catch (InvalidVersionSpecificationException e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }
    }

    public static class PluginVersionRecommender implements ConfigDef.Recommender {

        Plugins plugins;

        public PluginVersionRecommender() {
            setPlugins(null);
        }

        void setPlugins(Plugins plugins) {
            this.plugins = plugins;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return null;
            }
            switch (name) {
                case ConnectorConfig.CONNECTOR_VERSION:
                    Class connectorClass = parsedConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG).getClass();
                    if (connectorClass == null) {
                        //should never happen
                        throw new ConfigException("Connector class is not set");
                    }
                    switch (ConnectorType.from(connectorClass)) {
                        case SOURCE:
                            return plugins.sourceConnectors(connectorClass.getName()).stream()
                                    .map(PluginDesc::version).collect(Collectors.toList());
                        case SINK:
                            return plugins.sinkConnectors(connectorClass.getName()).stream()
                                    .map(PluginDesc::version).collect(Collectors.toList());
                    }
                default:
                    throw new IllegalArgumentException("Unsupported config: " + name);
            }
        }


        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }

}
