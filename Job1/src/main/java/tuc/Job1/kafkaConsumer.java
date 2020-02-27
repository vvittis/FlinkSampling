package tuc.Job1;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

public class kafkaConsumer {
    
 private FlinkKafkaConsumer<String> fc;
	


	public kafkaConsumer(String server, String topic) {
	Properties properties = new Properties();
	
	
	properties.setProperty("bootstrap.servers", server); 
	properties.setProperty("group.id", "test");
	
	fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties).setStartFromEarliest();
	
	}

	public void cancel() {
		
		fc.cancel();
	
	}
	
	public FlinkKafkaConsumer<String> getFc() {
	return fc;
}

public void setFc(FlinkKafkaConsumer<String> fc) {
	this.fc = fc;
}
}
