package tuc.Job1;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import tuc.Job1.kafkaConsumer;
import tuc.Job1.kafkaProducer2;

public class Job1 {
	
	
	public static void main(String[] args) throws Exception {

		// Set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		final ParameterTool params = ParameterTool.fromArgs(args);
	    env.setParallelism(Integer.parseInt(params.get("parallelism")));
	    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
	    env.getConfig().setGlobalJobParameters(params);
	    
		// Using KafkaTopics as Source and Sink
		String kafkaDataInputTopic = "testSource";
		String kafkaBrokersList = "localhost:9092";
		String kafkaOutputTopic = "testSink";
		
		kafkaConsumer kc = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaProducer2 kp = new kafkaProducer2(kafkaBrokersList, kafkaOutputTopic);

		DataStream<String> datastream = env.addSource(kc.getFc());
		//datastream.print();
		
		DataStream<Tuple2<String,Double>> mapped = datastream.map(new MapFunction<String, Tuple2<String, Double>>(){
			public Tuple2<String, Double> map(String value) {
				String[] col = params.get("columns").split(",");
				String[] group = params.get("group_attr").split(",");
				String aggr = params.get("aggr_attr");
				String[] words = value.split(",");
				
				int[] groupattr = findGroupAttr(col,group);
				int aggrAttr = findAggrAttr(col,aggr);
				
				String keys="";
				for(int i=0;i<groupattr.length;i++) {
					keys = keys + words[groupattr[i]]+",";
				}
				
				keys = keys.substring(0, keys.length()-1);
				
				return new Tuple2<String, Double>(keys, Double.parseDouble(words[aggrAttr]));
			}
		});
		//mapped.print();

		DataStream<Tuple5<String,Double,Double,Double,Integer>> counts = mapped.keyBy(new MyKeySelector1()).window(TumblingProcessingTimeWindows.of(Time.seconds(Integer.parseInt(params.get("windowTime"))))).process(new MyProcessWindowFunction());
		//counts.print();
		
		DataStream<Tuple5<String,Double,Double,Double,Integer>> finaldata = counts.keyBy(new MyKeySelector2()).window(TumblingProcessingTimeWindows.of(Time.seconds(Integer.parseInt(params.get("windowTime1"))))).process(new MyProcessWindowFunction1());
		finaldata.print();
		
		//finaldata.print();
		
		finaldata.addSink(kp.getProducer()); // added

		
		// Emit result
//		String dir = params.get("dir");
//		dir = dir+"\\finaldata.txt";
//		finaldata.writeAsText(dir,WriteMode.OVERWRITE).setParallelism(1);
		
		// Execute program
		env.execute("Streaming WordCount");
		
	}
	
	
	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	
	public static class MyKeySelector1 implements KeySelector<Tuple2<String,Double>,String>{

		@Override
		public String getKey(Tuple2<String,Double> value) throws Exception {
			// TODO Auto-generated method stub
			return value.f0;
		}
		
	}

	
	public static class MyKeySelector2 implements KeySelector<Tuple5<String, Double, Double,Double, Integer>,Integer>{

		@Override
		public Integer getKey(Tuple5<String, Double, Double, Double, Integer> value) throws Exception {
			// TODO Auto-generated method stub
			return value.f4;
		}
		
	}
	
	
	public static class Splitter implements MapFunction<String, Tuple2<String, Integer>> {
		
		public Tuple2<String, Integer> map(String value) {
			String[] words = value.split(",");
			//System.out.println("Words2 is "+words[2]+" and words7 is "+words[7]);
			//return new Tuple2<String, Integer>(words[0], Integer.parseInt(words[1]));
			//String[] groupbyattr = s.split(",");
			//String str="";
			//for(int i=0;i<groupbyattr.length;i++) {
			//	str = str + words[Integer.parseInt(groupbyattr[i])]+",";
			//}
			//str = str.substring(0, str.length()-1);
			//System.out.println("The str is :"+str);
			
			return new Tuple2<String, Integer>(words[2], Integer.parseInt(words[7]));
		}
	}
	
	
	public static int[] findGroupAttr(String[] columns , String[] group ) {
		
		int[] pos = new int[group.length];
		int k=0;
		
		//Find pos from columns
		for(int i=0;i<columns.length;i++) {
			for(int j=0;j<group.length;j++) {
				if(columns[i].equals(group[j])){
					pos[k]=i;
					k++;
					break;
				}
			}
		}
		
		
		return pos;
	
	}
	
	
	public static int findAggrAttr(String[] columns , String aggr ) {
		
		int k=0;
		
		//Find pos from columns
		for(int i=0;i<columns.length;i++) {
			if(columns[i].equals(aggr)){
				k=i;
				break;
			}
		}
		
		return k;
	}
	
	
	public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Double>,Tuple5<String,Double,Double,Double,Integer>, String, TimeWindow>{

		@Override
		public void process(String key,Context context,Iterable<Tuple2<String,Double>> input,Collector<Tuple5<String, Double, Double,Double, Integer>> out) throws Exception {
						 
			 //Mean
			 Double sum = 0.0;
			 Double count = 0.0;
			 Double mean=0.0;
			 for (Tuple2<String,Double> element : input) {
//				System.out.println(element.f0);
				sum+=element.f1;
				count++;
			 }
			 
//			 System.out.println(count);
			 //Variance
			 Double sumVar = 0.0;
			 for (Tuple2<String,Double> element : input) {
				 sumVar = sumVar + Math.pow(element.f1 - (sum/count), 2);
			 }
			 mean = sum/count;
			 sumVar = Math.sqrt(sumVar/count);
			 
			 out.collect(new Tuple5<String, Double, Double,Double, Integer>(input.iterator().next().f0,mean,sumVar,count,1));
			 
		}
		
	}
	
	
	public static class MyProcessWindowFunction1 extends ProcessWindowFunction<Tuple5<String,Double,Double,Double,Integer>,Tuple5<String,Double,Double,Double,Integer>,Integer, TimeWindow>{

		@Override
		public void process(Integer key,Context context,Iterable<Tuple5<String, Double, Double,Double, Integer>> input,Collector<Tuple5<String, Double, Double,Double, Integer>> out) throws Exception {
			
			//Calculate the size of stratum
			Double sumCV =0.0;
			for (Tuple5<String, Double, Double,Double, Integer> element : input) {
				
//				System.out.println("+++++++++++++++++++++++");
//				System.out.println(element.f1);
//				System.out.println(element.f2);
//				System.out.println("+++++++++++++++++++++++");
				
				 sumCV+=element.f2/element.f1;
			}		
//			System.out.println(sumCV);
			//Emit the final output
			for (Tuple5<String, Double, Double,Double, Integer> element : input) {
				ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
				int memory = Integer.parseInt(parameters.get("memory"));
				out.collect(new Tuple5<String, Double, Double,Double, Integer>(element.f0,element.f1,element.f2,element.f3,(int) Math.floor(   memory*(element.f2/element.f1)/sumCV      )             )               );
			}
			
		}
		
	}
	
	
	
}
