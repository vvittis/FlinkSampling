package tuc.Job1;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class kafkaProducer2 {
	
FlinkKafkaProducer<Tuple5<String, Double, Double,Double,Integer>> myProducer;
	
	
	public kafkaProducer2(String brokerlist, String outputTopic) {
		
		myProducer = new FlinkKafkaProducer<>(
					brokerlist,            // broker list
			        outputTopic,                  // target topic
			        (SerializationSchema<Tuple5<String, Double, Double,Double,Integer>>)new  AverageSerializer2()); 
			        //new SimpleStringSchema()); 
		myProducer.setWriteTimestampToKafka(true);		
	}
	
	public SinkFunction<Tuple5<String, Double, Double, Double, Integer>> getProducer(){
		return myProducer;
	}	
}
 class AverageSerializer2 implements SerializationSchema<Tuple5<String, Double, Double,Double,Integer>> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
/*
	@Override
    public byte[] serializeKey(Tuple2 element) {
      return ("\"" + element.getField(0).toString() + "\"").getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2 element) {
      String value = element.getField(1).toString();
      return value.getBytes();
    }
*/
  
    public String getTargetTopic(Tuple2<String, Integer> element) {
      // use always the default topic
      return null;
    }

	@Override
	public byte[] serialize(Tuple5<String, Double, Double,Double,Integer> element) {
		
		return (""+element.getField(0).toString()+ ","+element.getField(1).toString()+","+element.getField(2).toString()+","+element.getField(3).toString() +","+element.getField(4).toString()+"").getBytes();
	}
  }