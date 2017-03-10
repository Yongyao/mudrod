package esiptestbed.mudrod.streaming;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreaming {
  private static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
  
//  private static final Function2<Session, Session, Session> Session_Merger = 
//      new Function2<Session, Session, Session>() {
//    @Override public Session call(Session s1, Session s2) {
//      return s1.add(s2);
//    }
//  };
  
  private static final Function2<Session, Session, Session> Session_Merger = (s1, s2) -> Session.add(s1, s2);
  
  private static final Function2<List<Session>, Optional<Session>, Optional<Session>>
  COMPUTE_RUNNING_SESSION = (currents, old) -> {   
    Session s = old.orNull();
    for (Session i : currents) {
      s = Session.add(s, i);
    }
    return Optional.of(s);
  };
  
  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setAppName("MUDROD Streaming");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaStreamingContext jssc = new JavaStreamingContext(sc,
        new Duration(3000));  // This sets the update window to be every 3 seconds.
    
    jssc.checkpoint("checkpoints-mudrod-streaming-total");
     
    //tail -f [[YOUR_LOG_FILE]] | nc -lk 9999
    //nc -lk 9999
    //monitor folder
//    if(args.length==0){
//      System.out.println("Please input the log file path!");
//      return;
//    }
//    JavaDStream<String> logDataDStream = jssc.textFileStream(args[0]);
    JavaReceiverInputDStream<String> logDataDStream =
        jssc.socketTextStream("localhost", 9999);

    // A DStream of Apache Access Logs.
    JavaDStream<ApacheAccessLog> accessLogDStream =
        logDataDStream.map(ApacheAccessLog::parseFromLogLine)
        .filter(log->log.isCrawler()==false);
    
    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
        .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
        .reduceByKey(SUM_REDUCER);
    responseCodeCountDStream.foreachRDD(rdd -> {
      System.out.println("Response code counts: " + rdd.take(100));
    });
    
    // A DStream of ipAddresses accessed > 10 times.
    JavaPairDStream<String, Session> ipDStream = accessLogDStream
        .mapToPair(s -> new Tuple2<>(s.getIpAddress(), new Session(s)))
        .reduceByKey(Session_Merger)
        .updateStateByKey(COMPUTE_RUNNING_SESSION);
    
    ipDStream.foreachRDD(rdd -> {
      List<Tuple2<String, Session>> sessions = rdd.take(100);
      for(Tuple2<String, Session> t:sessions)
      {
        System.out.println(t._1 + " " + t._2.getLogList().size());
      }
    });
    
    // Start the streaming server.
    jssc.start();              // Start the computation
    jssc.awaitTermination();   // Wait for the computation to terminate
  }
}

