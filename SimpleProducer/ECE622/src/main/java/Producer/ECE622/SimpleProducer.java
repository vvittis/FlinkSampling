package Producer.ECE622;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SimpleProducer {
 
	 public static void main(String[] args) throws Exception{
	    
		 
	    //Assign topicName to string variable
	    String topicName = "testSource";
	    
	    //Create instance for properties to access producer configs   
	    Properties props = new Properties();
	    
	    
	    //Assign localhost id
	    props.put("bootstrap.servers", "localhost:9092");  
	    
	    
	    //Set acknowledgements for producer requests.      
	    props.put("acks", "all");
	    
	    
	    //If the request fails, the producer can automatically retry
	    props.put("retries", 0);    
	    
	       
	    //Specify buffer size in config
	    props.put("batch.size", 16384);
	    
	    
	    //Reduce the number of requests less than 0 
	    props.put("linger.ms", 0);
	    
	    
	    //The buffer.memory controls the total amount of memory available to the producer for buffering. 
	    props.put("buffer.memory", 33554432);
	    
	    
	    //key-serializer -> string
	    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    //value-serializer -> string
	    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	    
	    
	    //Producer
	    Producer<String, String> producer = new KafkaProducer<String, String>(props); 
	    
	    //Read the file line by line and send to topic topicName
	    BufferedReader br = new BufferedReader(new FileReader(args[0])); // args[0]->input file
	    String line = br.readLine();
	    int count = 0;
	    while (line != null) {
	        count++;
		    producer.send(new ProducerRecord<String, String>(topicName,String.valueOf(count),line));   
		    line = br.readLine();
		}
	    
	    br.close();
	    producer.close();
	             
	 }
	 
	 
}
