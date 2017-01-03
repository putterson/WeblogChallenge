import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by patcgoe on 11/7/16.
  */


object WeblogChallenge {

  //Structure of elb access log entries
  case class LogEntry(timestamp: Timestamp,
                      elb: String,
                      client_ipport: String,
                      backend_ipport: String,
                      request_processing_time: Double,
                      backend_processing_time: Double,
                      response_processing_time: Double,
                      elb_status_code: Int,
                      backend_status_code: Int,
                      received_bytes: Int,
                      sent_bytes: Int,
                      request: String,
                      user_agent: String,
                      ssl_cipher: String,
                      ssl_protocol: String,
                      client_ip: String)

  case class LogEntryKey(client_ip: String, timestamp: Timestamp)

  object LogEntryKey {
    implicit def timeStampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_ before _)

    implicit def logEntryOrdering[A <: LogEntryKey]: Ordering[A] = Ordering.by(logEntry => (logEntry.client_ip, logEntry.timestamp))
  }

  val IP_IDX = 2;
  val DATETIME_IDX = 1;

  //This threshold is in milliseconds
  val TIME_THRESHOLD = 15 * 60 * 1000;

  //Fifteen minutes between requests mean they are in the same session


  case class Session(client_ip: String,
                     start_timestamp: Timestamp,
                     end_timestamp: Timestamp,
                     entries: List[LogEntry]) {
    def this(entry: LogEntry) = this(entry.client_ip, entry.timestamp, entry.timestamp, entry :: Nil)
  }

  def main(args: Array[String]): Unit = {
    val logFile = "/home/patcgoe/Workspace/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log" // Should be some file on your system
    //    val logFile = "/home/patcgoe/Workspace/WeblogChallenge/data/small.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Weblog Challenge Application").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().config(conf).getOrCreate()
    import org.apache.spark.sql.catalyst.ScalaReflection
    import ss.implicits._
    val logSchema = ScalaReflection.schemaFor[LogEntry].dataType.asInstanceOf[StructType]

    val logDataFrame = ss.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(logSchema)
      .csv(logFile)

    import org.apache.spark.sql.functions._
    val parseIP = udf((ipport: String) => ipport.split(":")(0))

    val logDataSet = logDataFrame.withColumn("client_ip", parseIP(col("client_ipport"))).as[LogEntry]

    val logRDD = logDataSet.rdd

    def entryInSession(session: Session, entry: LogEntry): Boolean = {
      if (session.client_ip != entry.client_ip) {
        return false
      }
      val forwardTime = new Timestamp(entry.timestamp.getTime() + TIME_THRESHOLD)
      val backwardTime = new Timestamp(entry.timestamp.getTime() - TIME_THRESHOLD)
      forwardTime.after(session.start_timestamp) && backwardTime.before(session.end_timestamp)
    }

    def mergeEntryIntoSession(session: Session, entry: LogEntry): Session = {
      implicit def timeStampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_ before _)
      session.copy(
        start_timestamp = (session.start_timestamp :: entry.timestamp :: Nil).min,
        end_timestamp = (session.end_timestamp :: entry.timestamp :: Nil).max,
        entries = entry :: session.entries);
    }

    def mergeEntryIntoSessions(sessions: List[Session], entry: (LogEntryKey, LogEntry)): List[Session] = {
      implicit def timeStampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_ before _)

      sessions match {
        case Nil => new Session(entry._2) :: Nil
        case (x: Session) :: xs => entryInSession(x, entry._2) match {
          case true => mergeEntryIntoSession(x, entry._2) :: xs
          case false => new Session(entry._2) :: x :: xs
        }
      }
    }

    class LogEntryKeyIPPartitioner(partitions: Int) extends Partitioner {
      require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[LogEntryKey]
        Math.abs(k.client_ip.hashCode()) % numPartitions
      }
    }

    val orderedRDD = logRDD.map(e => (LogEntryKey(e.client_ip, e.timestamp), e))
    val sortedRDD = orderedRDD.repartitionAndSortWithinPartitions(new LogEntryKeyIPPartitioner(128))

    //Goal 1. aggregated holds a list of Session objects, each representing one contiguous session of client activity
    val aggregated = sortedRDD.aggregate(List[Session]())(mergeEntryIntoSessions, _ ++ _)
    val aggregatedRDD = sc.parallelize(aggregated)
    aggregatedRDD.toDF().write.json("data/goal1.json")

    //Goal 2.
    val countAndTime = aggregatedRDD.aggregate((0D, 0D))((t, s) => (t._1 + 1, t._2 + (s.end_timestamp.getTime - s.start_timestamp.getTime)), (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
    val averageSessionTimeInSeconds: Double = (countAndTime._2 / countAndTime._1) / 1000D
    println(f"Goal 2: The mean session time in seconds is $averageSessionTimeInSeconds%.2f")

    //Goal 3.
    val sessionRequestSets = aggregatedRDD.map((s) => (s, s.entries.map((e) => e.request).toSet.size))
    sessionRequestSets.toDF().write.json("data/goal3.json")

    //Goal 4.
    implicit object SessionLengthTupleOrd extends math.Ordering[(Long, Session)] {
      def compare(x: (Long, Session), y: (Long, Session)): Int = {
        x._1.compare(y._1);
      }
    }

    val topTenSessions = aggregatedRDD.map((s) => (s.end_timestamp.getTime - s.start_timestamp.getTime, s)).top(10)
    sc.parallelize(topTenSessions).toDF().write.json("data/goal4.json")
  }
}
