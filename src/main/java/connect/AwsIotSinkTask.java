package kafka.connect.awsiot;

import java.util.Collection;
import java.util.Map;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.RetriableException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.amazonaws.services.iot.*;
import com.amazonaws.services.iotdata.*;
import com.amazonaws.services.iotdata.model.*;

public class AwsIotSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(AwsIotSinkTask.class);

    private AWSIotData iot = null;
    private ObjectMapper jsonMapper;
    ObjectNode awsTemplateNode;

    public AwsIotSinkTask() {
	jsonMapper = new ObjectMapper();
    }

    @Override
    public String version() {
	return new AwsIotSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
	log.info("Starting task");

	iot = AWSIotDataClientBuilder.defaultClient();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {

	if (sinkRecords.isEmpty()) {
	    log.warn("No Records to process");
	    return;
	}

	SinkRecord first = sinkRecords.iterator().next();
	log.warn("Received {} records. Topic-Partition-Offset: {}-{}-{}", sinkRecords.size(),
		  first.topic(), first.kafkaPartition(), first.kafkaOffset());

	for (SinkRecord record: sinkRecords) {
	    JsonNode parsedJson;
	    try {
		 parsedJson = jsonMapper.readTree((byte[]) record.value());
	    } catch (JsonProcessingException e) {
		log.warn("Dropping invalid JSON (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    } catch (IOException e) {
		log.warn("Dropping because of unhandled exception (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    }

	    String thingName = parsedJson.get("DeviceId").asText();

	    ObjectNode stateNode = jsonMapper.createObjectNode();
	    stateNode.set("reported", parsedJson.get("Message"));
	    ObjectNode rootNode = jsonMapper.createObjectNode();
	    rootNode.set("state", stateNode);

	    byte[] awsPayload;
	    try {
		awsPayload = jsonMapper.writeValueAsBytes(rootNode);
	    } catch (JsonProcessingException e) {
		log.warn("Dropping message because we are nable to create Json Object to send to AWS (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    }
	    UpdateThingShadowRequest utsr = new UpdateThingShadowRequest()
		.withThingName(thingName)
		.withPayload(ByteBuffer.wrap(awsPayload));

	    try {
		iot.updateThingShadow(utsr);
	    } catch (ThrottlingException e) {
		throw new RetriableException("The rate exceeds the limit, will retry ...");
	    } catch (ServiceUnavailableException e) {
		throw new RetriableException("The AWS Iot Service is currently unavailable, will retry ...");
	    } catch (Exception e) {
		log.warn("Error while updating the device (Topic-Partition-Offset: {}-{}-{}): {}",
			 record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.toString());
		continue;
	    }
	}
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
	log.debug("flush() called");
    }

    @Override
    public void stop() {
	log.debug("stop() called");
	iot.shutdown();
    }
}

