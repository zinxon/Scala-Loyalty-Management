import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object KafkaIOStream extends Serializable{
  def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    val conf = ConfigFactory.load
    val spark = SparkSession.builder()
      .appName("Kafka Streaming")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.streaming.stopGracefullyOnShutDown", value = true)
      .getOrCreate()

    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))

    val kafkaSourceDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("kafka.broker"))
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()

    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), schema).alias("value"))

    val notificationDF = valueDF.select("value.InvoiceNumber","value.CustomerCardNo","value.TotalAmount")
      .withColumn("EarnedLoyaltyPoints",expr("TotalAmount*0.2"))

    val kafkaTargetDF = notificationDF.selectExpr("InvoiceNumber as key",
      """to_json(named_struct('CustomerCardNo', CustomerCardNo,
        |'TotalAmount', TotalAmount,
        |'EarnedLoyaltyPoints', TotalAmount * 0.2
        |)) as value""".stripMargin)

    val notificationWriterQuery = kafkaTargetDF.writeStream
      .format("kafka")
      .queryName("Notification Writer")
      .option("kafka.bootstrap.servers", conf.getString("kafka.broker"))
      .option("topic", "notifications")
      .option("checkpointLocation", "chk-point-dir")
      .outputMode("append")
      .start()

    logger.info(s"Listening and writing to ${conf.getString("kafka.broker")}")
    notificationWriterQuery.awaitTermination()
  }
}
