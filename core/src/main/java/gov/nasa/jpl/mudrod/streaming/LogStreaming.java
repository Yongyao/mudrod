package gov.nasa.jpl.mudrod.streaming;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import scala.Tuple2;

public class LogStreaming {
  private static final int interval_timeout = Integer.MAX_VALUE;
  private static final Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

  //  private static final Function2<Session, Session, Session> Session_Merger = 
  //      new Function2<Session, Session, Session>() {
  //    @Override public Session call(Session s1, Session s2) {
  //      return s1.add(s2);
  //    }
  //  };

  private static final Function2<Session, Session, Session> Session_Merger = (s1, s2) -> Session.add(s1, s2);

  private static final Function2<List<Session>, Optional<Session>, Optional<Session>> COMPUTE_RUNNING_SESSION = 
      (news, current) -> {   
        Session s = current.orNull();
        DateTime now = DateTime.now(); //change the value of now to test
        if(s!=null)
        {
//          System.out.println(now.toString());
//          System.out.println(s.getEndTimeObj().toString());
          int interval = Seconds.secondsBetween(s.getEndTimeObj(), now).getSeconds();
          //System.out.println(interval);
          if(interval > interval_timeout)
          {
            //SessionProcesser.process(s);
            if(news.size()>0)
            {
              for (Session i : news) 
              {     
                s = Session.add(null, i);
              } 
              return Optional.of(s);
            }else
            {
              return Optional.absent();
            }
          }
        }

        for (Session i : news) {     
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
    //http://stackoverflow.com/questions/36421619/whats-the-meaning-of-dstream-foreachrdd-function/42771764#42771764
    JavaDStream<ApacheAccessLog> accessLogDStream =
        logDataDStream.map(ApacheAccessLog::parseFromLogLine)
        .filter(log->log.isSearchLog()==true)
        .filter(log->log.isCrawler()==false);

//    JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogDStream
//        .mapToPair(s -> new Tuple2<>(s.getResponseCode(), 1L))
//        .reduceByKey(SUM_REDUCER);
//    responseCodeCountDStream.foreachRDD(rdd -> {
//      System.out.println("Response code counts: " + rdd.take(100));
//    });

    // A DStream of sessions with ip being the key
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
/*notes: 1. pass -Dspark.master=local[*] to vm arguments 2. the interval_timeout needs to be changed in production
 * 3. currently, it seems to only support terminal monitoring*/


