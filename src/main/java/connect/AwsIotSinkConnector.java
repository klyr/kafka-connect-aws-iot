package kafka.connect.awsiot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.ConnectorContext;

public class AwsIotSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(AwsIotSinkConnector.class);

    public static final String AWS_IOT_CONNECT_VERSION = "0.1";

    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    /*
	.define(FUSEKI_SERVER, Type.STRING, null, Importance.HIGH, "Fuseki server")
	.define(FUSEKI_DATASET, Type.STRING, null, Importance.HIGH, "Fuseki dataset");
    
    private String fusekiServer;
    private String fusekiDataset;
    */

    @Override
    public void initialize(ConnectorContext context) {
    }

    @Override
    public ConfigDef config() {
	    return CONFIG_DEF;
    }

    @Override
    public void start(final Map<String, String> props) {
	log.info("====== Starting Connector ======");

	/*
	fusekiServer = props.get(FUSEKI_SERVER);
	fusekiDataset = props.get(FUSEKI_DATASET);
	*/
    }

    @Override
    public void stop() {
	// Nothing
    }

    @Override
    public Class<? extends Task> taskClass() {
	return AwsIotSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
	ArrayList<Map<String, String>> configs = new ArrayList<>();
	for (int i = 0; i < maxTasks; i++) {
	    Map<String, String> config = new HashMap<>();
	    /*
	    config.put(FUSEKI_SERVER, fusekiServer);
	    config.put(FUSEKI_DATASET, fusekiDataset);
	    */
	    configs.add(config);
	}
	return configs;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
