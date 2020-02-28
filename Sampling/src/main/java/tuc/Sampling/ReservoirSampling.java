package tuc.Sampling;


import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import tuc.Sampling.kafkaProducer;
import tuc.Sampling.kafkaConsumer;


public class ReservoirSampling{

	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment  env = StreamExecutionEnvironment.getExecutionEnvironment();
	    
	    ParameterTool params = ParameterTool.fromArgs(args);
	    	    
	    env.setParallelism(Integer.parseInt(params.get("parallelism")));
	    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
	    env.getConfig().setGlobalJobParameters(params);
	    
	    String kafkaDataInputTopic = "testSource";
		String kafkaBrokersList = "localhost:9092";
		String kafkaDataInputTopicJob1 = "testSink";
		
		kafkaConsumer kc = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopic);
		kafkaProducer kp = new kafkaProducer(kafkaBrokersList, params.get("output"));
		kafkaConsumer kcJob1 = new kafkaConsumer(kafkaBrokersList, kafkaDataInputTopicJob1);
		
		
	    //Read the broadcasted data
	    DataStream<Tuple3<String, Double, Integer>> broadCastData = env.addSource(kcJob1.getFc()).map(new SplitterBroadCast()).name("broadCastData");
	    //broadCastData.print();
	    
	    
	    //BroadcastStream
	    MapStateDescriptor<String, Tuple3<String, Double, Integer>> broadCastDesc = new MapStateDescriptor<String,Tuple3<String, Double, Integer>>("broadCastDesc",BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.of(new TypeHint<Tuple3<String, Double, Integer>>() {}) );
	    BroadcastStream<Tuple3<String, Double, Integer>> broadCastStream = broadCastData.broadcast(broadCastDesc);
	    
	    
	    //Non-BroadcastStream
	    DataStream<String> datastream = env.addSource(kc.getFc()).name("testSource");
	    DataStream<Tuple2<String, String>> mapped = datastream.map(new MapFunction<String, Tuple2<String,String>>(){

			@Override
			//key,tuple
			public Tuple2<String, String> map(String value) throws Exception {
				
				String[] col = params.get("columns").split(",");
				String[] group = params.get("group_attr").split(",");
				String[] words = value.split(",");
				
				int[] groupattr = findGroupAttr(col,group);

				String keys="";
				for(int i=0;i<groupattr.length;i++) {
					keys = keys + words[groupattr[i]]+",";
				}
				
				keys = keys.substring(0, keys.length()-1);
				
				return new Tuple2<String, String>(keys,value);
			}
	    	
	    });
	    //mapped.print();
	    
	    
	    //Connect and process the broadCastStream and datastream
	  	DataStream<String> finalRes = mapped.keyBy(0).connect(broadCastStream).process(new BroadCastFunc());
	    //finalRes.print();
	  	
	  	
	  	// Emit result
	    finalRes.addSink(kp.getProducer()).name(params.get("output"));
	  	
	  	// Execute program
	    env.execute("Reservoir Sampling");
	    	    
	}
	
	
	// *************************************************************************
	//  							FUNCTIONS
	// *************************************************************************
	
	public static class SplitterBroadCast implements MapFunction<String,Tuple3<String, Double, Integer>> {
		
		//key,sizeOfStratum,countOfGroupBy
		public Tuple3<String, Double, Integer> map(String value) {
			String[] words = value.split(",");
			
			String keys="";
			for(int i=0;i<words.length-4;i++) {
				keys = keys + words[i]+",";
			}
			keys = keys.substring(0, keys.length()-1);
			return new Tuple3<String, Double, Integer>(keys,Double.parseDouble(words[words.length-2]),Integer.parseInt(words[words.length-1]));
		}
	}
	
		
	public static int[] findGroupAttr(String[] columns , String[] group ) {
		
		int[] pos = new int[group.length];
		int k=0;
		
		//Find pos-groupby_attr from columns
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

	
	public static class BroadCastFunc extends KeyedBroadcastProcessFunction<
	String, //the key type of keyed input stream(non-broadcast stream)
	Tuple2<String, String>, //non-broadcast stream
	Tuple3<String, Double, Integer>, // broadcast stream
	String //output
	> {
		
		private transient ValueState<Integer> count; // counter
		private transient ValueState<Integer> sizeOfStratum; // size Of Stratum
		private transient ValueState<Boolean> finded; // help value state
		private transient ValueState<Boolean> findedSize; // finded size of stratum
		private transient ValueState<Double> countOfGroupBy; // count Of GroupBy
		private transient ListState<String> delayedList; // delayed list
		
		private final MapStateDescriptor<String, ArrayList<String>> listOfTuples = new MapStateDescriptor<String,ArrayList<String>>("listOfTuples",BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.of(new TypeHint<ArrayList<String>>() {}) );  // list of tuples 		
		private final MapStateDescriptor<String, Tuple3<String, Double, Integer>> broadCastDesc = new MapStateDescriptor<String,Tuple3<String, Double, Integer>>("broadCastDesc",BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.of(new TypeHint<Tuple3<String, Double, Integer>>() {}) ); // broadcast descriptor		
					
		
		public void processElement(Tuple2<String, String> input, ReadOnlyContext ctx, Collector<String> out) throws Exception
		{
			
			final MapState<String, ArrayList<String>> state = getRuntimeContext().getMapState(listOfTuples);
			 
			//Update counter
			Integer currCount = count.value();
			currCount++;
			count.update(currCount);
			
			
			// get key of current tuple
	    	final String cId = input.f0;
	    	
	    	
	    	//Search on broadcasted stream,find the size of stratum and count Of GroupBy
	    	if(finded.value()){
				for (Map.Entry<String, Tuple3<String, Double, Integer>> broadEntry: ctx.getBroadcastState(broadCastDesc).immutableEntries()){				    	
				    	if(broadEntry.getKey().equals(cId)){
				    		sizeOfStratum.update(broadEntry.getValue().f2);
				    		countOfGroupBy.update(broadEntry.getValue().f1);				
				    		finded.update(false);
				    		break;
				    	}	
				}
	    	}
	    	
	    	
	    	//Delayed tuple from broadcast stream
	    	if(sizeOfStratum.value()==0){
	    		delayedList.add(input.f1);
	    		//size of stratum is zero
	    		if(currCount == (int) Math.round(countOfGroupBy.value()) ){
	    			delayedList.clear();
	    		}
	    		return;
	    	}
	    	else if(findedSize.value() && sizeOfStratum.value()!=0){
	    		delayedList.add(input.f1);
		    	ArrayList<String> listTuples = state.get(input.f0);
		    	if(listTuples == null) {  listTuples = new ArrayList<String>(); }
		    	
		    	//Reservoir Sampling
		    	for (String tuple : delayedList.get()){
		    		if(listTuples.size()<sizeOfStratum.value()) {
		    			listTuples.add(tuple);
		    		}
		    		else {
		    			Random r = new Random(); 
				    	int j = r.nextInt(listTuples.size() + 1);
				        if (j < sizeOfStratum.value()){
				        	listTuples.set(j, input.f1);
				        }
		    		}
		    	}
		    			   
		    	
		    	//Emit the result
		    	if(currCount == (int) Math.round(countOfGroupBy.value()) ){			    	
			    	for(int i=0;i<listTuples.size();i++) {
			    		out.collect(listTuples.get(i));
			    	}
			    	delayedList.clear();
			    	listTuples.clear();
			    	return;
		    	}
		    	
		    	state.put(input.f0,listTuples);
		    	delayedList.clear();
		    	findedSize.update(false);
		    	return;
	    	}

	    	
	    	//Non-Delayed tuple from broadcast stream	    
		    ArrayList<String> list = state.get(input.f0); // get the list for the input tuple
		    if(list == null) {  list = new ArrayList<String>(); }
		    
		    //Reservoir Sampling
		    if(count.value()<=sizeOfStratum.value()) {
		    	list.add(input.f1); // add tuple at list
		    	state.put(input.f0,list); // update state
		    }
		    else{
		    	Random r = new Random(); 
		    	int j = r.nextInt(currCount + 1);
		        if (j < sizeOfStratum.value()) {
		        	list.set(j, input.f1);  // add tuple at list
		        	state.put(input.f0,list); // update state
		        }
		        
		    }
		    
		    //Emit the result
		    if(currCount == (int) Math.round(countOfGroupBy.value()) ){		    
		    	for(int i=0;i<list.size();i++) {
		    		out.collect(list.get(i));
		    	}
		    	getRuntimeContext().getMapState(listOfTuples).get(input.f0).clear();
		    }

		}
		
		
		public void processBroadcastElement(Tuple3<String, Double, Integer> broadData, Context ctx, Collector<String> out) throws Exception
		{					

			String id = broadData.f0; //key
		    ctx.getBroadcastState(broadCastDesc).put(id, broadData);
		} 
		
		
		public void open(Configuration conf)
		{
			ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<Integer>("count", BasicTypeInfo.INT_TYPE_INFO, 0);
			count = getRuntimeContext().getState(desc);
			
			ValueStateDescriptor<Integer> desc1 = new ValueStateDescriptor<Integer>("sizeOfStratum", BasicTypeInfo.INT_TYPE_INFO, 0);
			sizeOfStratum = getRuntimeContext().getState(desc1);
			
			ValueStateDescriptor<Boolean> desc2 = new ValueStateDescriptor<Boolean>("finded", BasicTypeInfo.BOOLEAN_TYPE_INFO, true);
			finded = getRuntimeContext().getState(desc2);
			
			ValueStateDescriptor<Double> desc3 = new ValueStateDescriptor<Double>("countOfGroupBy", BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);
			countOfGroupBy = getRuntimeContext().getState(desc3);
			
			ListStateDescriptor<String> listDesc = new ListStateDescriptor<String>("delayedList", TypeInformation.of(new TypeHint<String>() {}));
			delayedList = getRuntimeContext().getListState(listDesc);
			
			ValueStateDescriptor<Boolean> desc4 = new ValueStateDescriptor<Boolean>("findedSize", BasicTypeInfo.BOOLEAN_TYPE_INFO, true);
			findedSize = getRuntimeContext().getState(desc4);
			
		    
		}

		
	}
	
	
}
