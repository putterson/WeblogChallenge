import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

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

  def main(args: Array[String]): Unit = {
    val logFile = "/home/patcgoe/Workspace/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log" // Should be some file on your system
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

    logDataSet.show()
  }
}
