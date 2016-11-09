import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.immutable.HashMap

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

  val IP_IDX = 2;
  val DATETIME_IDX = 1;
  val TIME_THRESHOLD = 60;


  case class Session(client_ip : String,
                     start_timestamp : Timestamp,
                     end_timestamp: Timestamp,
                     entries: List[LogEntry]){
    def this(entry : LogEntry) = this(entry.client_ip, entry.timestamp, entry.timestamp, entry :: Nil)
  }


  def main(args: Array[String]): Unit = {
//    val logFile = "/home/patcgoe/Workspace/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log" // Should be some file on your system
    val logFile = "/home/patcgoe/Workspace/WeblogChallenge/data/small.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Weblog Challenge Application").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().config(conf).getOrCreate()
    import ss.implicits._

    import org.apache.spark.sql.catalyst.ScalaReflection
    val logSchema = ScalaReflection.schemaFor[LogEntry].dataType.asInstanceOf[StructType]

    val logDataFrame = ss.read
      .option("delimiter", " ")
      .option("header", "false")
      .schema(logSchema)
      .csv(logFile)

    import org.apache.spark.sql.functions._
    val parseIP = udf((ipport : String) => ipport.split(":")(0))

    val logDataSet = logDataFrame.withColumn("client_ip", parseIP(col("client_ipport"))).as[LogEntry]

    val logRDD = logDataSet.rdd

    def entryInSession(session: Session, entry: LogEntry): Boolean = {
      if (session.client_ip != entry.client_ip) { return false }
      val forwardTime = new Timestamp(entry.timestamp.getTime() + TIME_THRESHOLD)
      val backwardTime = new Timestamp(entry.timestamp.getTime() - TIME_THRESHOLD)
      forwardTime.after(session.start_timestamp) && backwardTime.before(session.end_timestamp)
    }

    def mergeEntryIntoSessions(sessions: List[Session], entry: LogEntry): List[Session] = {
      implicit def timeStampOrdering: Ordering[Timestamp] = Ordering.fromLessThan(_ before _)

      val newSession = sessions.filter(session => session.client_ip == entry.client_ip).find(session => entryInSession(session, entry)) match {
        case Some(session) => session.copy(
          start_timestamp = (session.start_timestamp :: entry.timestamp :: Nil).min,
          end_timestamp = (session.start_timestamp :: entry.timestamp :: Nil).max,
          entries = entry :: session.entries);
        case None => new Session(entry);
      }
      newSession :: sessions
    }

    def contiguousSessions(s1 : Session, s2 : Session): Boolean = {
      (s1.client_ip == s2.client_ip) &&
        (s1.start_timestamp.before(new Timestamp(s2.end_timestamp.getTime() + TIME_THRESHOLD)) ||
          s1.end_timestamp.after(new Timestamp(s2.start_timestamp.getTime() - TIME_THRESHOLD)))

    }

//    def mergeSessions(s1 : Session, s2: Session) = {
//      s1.copy(
//        start_timestamp = (s1.start_timestamp :: s2.start_timestamp :: Nil).min,
//        end_timestamp = (s1.end_timestamp :: s2.end_timestamp :: Nil).max,
//        entries = s1.entries ++ s2.entries);
//    }

    //Sessions are by ip so don't need to be merged beyond concactenation
    def mergeSessionLists(sl1 : List[Session], sl2: List[Session]) : List[Session] = {
      sl1.union(sl2)
//      val add = (sessions : List[Session], session : Session) =>
//        sessions.find((s1) => contiguousSessions(s1, session)) match {
//          case Some(matching) => mergeSessions(matching, session)
//          case None => session :: sessions
//        }
//      val merge = (ss1 : List[Session], ss2 : List[Session]) => ss1 ++ ss2 : List[Session]
//
//      sl1.union(sl2).aggregate(Nil : List[Session])(add, merge)
    }

    logRDD.aggregate(List[Session]())(mergeEntryIntoSessions, mergeSessionLists)
      .toDS().write.json("/home/patcgoe/Workspace/WeblogChallenge/data/output.json")
//    val contiguousLogEntry = (l1 : LogEntry, l2 : LogEntry) => if(l1.timestamp.)

//    Window.partitionBy("client_ip").orderBy("timestamp").()show()


  }
}
