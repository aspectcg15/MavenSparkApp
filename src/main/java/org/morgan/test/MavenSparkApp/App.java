package org.morgan.test.MavenSparkApp;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.examples.StreamingExamples;
import org.apache.spark.streaming.kafka.KafkaUtils;


import scala.Tuple2;

import com.google.common.collect.Lists;


/**
 * Hello world!
 *
 */
public final class App 
{
	private static final Pattern SPACE=Pattern.compile(" ");
	
	private App() {
		
	}
	
    public static void main( String[] args )
    {
//    	if (args.length< 4) {
//    		System.err.print("Usage: JavaKafkaWordCount zookeeper 10.153.7.113:2181 test-consumer-group test, test1 1");
//    		System.exit(1);
//    	}
//        StreamingExamples.setStreamingLogLevels();
        SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("spark://10.153.7.113:7077");
        JavaStreamingContext jssc= new JavaStreamingContext(sparkConf, new Duration(2000));
        
        int numThreads= 1;
        Map<String, Integer> topicMap= new HashMap<String, Integer>();
        String[] topics= "page_visits".split(",");
        for(String topic : topics) {
        	topicMap.put(topic, numThreads);
        }
        
        JavaPairReceiverInputDStream<String, String> message = KafkaUtils.createStream(jssc, "10.153.7.113:2183", "test-consumer-group", topicMap);
        
        JavaDStream<String> lines = message.map(new Function<Tuple2<String, String>,String>(){
        	public String call(Tuple2<String, String> tuple2){
        		return tuple2._2();
        	}
        });
        
        JavaDStream<String> words= lines.flatMap(new FlatMapFunction<String, String>(){
        	
        	public Iterable<String> call(String x) {
        		return Lists.newArrayList(SPACE.split(x));
        	}
        });
        
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
        		new PairFunction<String,String, Integer>(){
        			
        			public Tuple2<String,Integer>call(String s) {
        				return new Tuple2<String, Integer>(s,1);
        			}
        		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
        			
        			public Integer call(Integer i1, Integer i2) {
        				return i1 + i2;
        			}
        		});
        message.print();
        //wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
