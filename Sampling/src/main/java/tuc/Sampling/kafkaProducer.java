package tuc.Sampling;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class kafkaProducer{
	
	FlinkKafkaProducer<String> myProducer;
	
	//Constructor kafkaProducer
	public kafkaProducer(String brokerlist, String outputTopic) {
		
		myProducer = new FlinkKafkaProducer<>(brokerlist,outputTopic,(SerializationSchema<String>)new  SimpleSerializer()); 
		myProducer.setWriteTimestampToKafka(true);		
	}
	
	
	public SinkFunction<String> getProducer(){
		return myProducer;
	}	
	
}

	//Serializer
   class SimpleSerializer implements SerializationSchema<String>{

	   	private static final long serialVersionUID = 1L;

		public byte[] serialize(String element) {
			return element.getBytes();
		}
	 
   }
   
   