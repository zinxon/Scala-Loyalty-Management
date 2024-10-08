import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types.{
  ArrayType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}

object KafkaIOStream extends Serializable {
  // Define the schema of the data
    private val invoiceSchema = StructType(
      List(
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
        StructField(
          "DeliveryAddress",
          StructType(
            List(
              StructField("AddressLine", StringType),
              StructField("City", StringType),
              StructField("State", StringType),
              StructField("PinCode", StringType),
              StructField("ContactNumber", StringType)
            )
          )
        ),
        StructField(
          "InvoiceLineItems",
          ArrayType(
            StructType(
              List(
                StructField("ItemCode", StringType),
                StructField("ItemDescription", StringType),
                StructField("ItemPrice", DoubleType),
                StructField("ItemQty", IntegerType),
                StructField("TotalValue", DoubleType)
              )
            )
          )
        )
      )
    )

  def main(args: Array[String]): Unit = {

    // Create a logger instance
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    // Load the configuration
    val conf = ConfigFactory.load

    // Create a SparkSession instance
    val spark = SparkSession
      .builder()
      .appName("Kafka Streaming")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.streaming.stopGracefullyOnShutDown", value = true)
      .getOrCreate()

    // Read data from the Kafka topic
    val kafkaSourceDF = readInvoiceFromKafka(spark,conf)
      
    // Parse the JSON data
    val valueDF = parseJsonData(kafkaSourceDF)
      writeNotifications(valueDF,conf)
    writeFlattenedInvoices(valueDF)

    // Log a message indicating that the application is running
    logger.info(s"Listening and writing to ${conf.getString("kafka.broker")}")

    // Wait for the notification writer query to finish
    spark.streams.awaitAnyTermination()
  }

  // Function to read data from Kafka
  private def readInvoiceFromKafka(spark: SparkSession, conf: com.typesafe.config.Config): org.apache.spark.sql.DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("kafka.broker"))
      .option("subscribe", "invoices")
      .option("startingOffsets", "earliest")
      .load()
  }

  // Function to parse JSON data using the defined schema
  private def parseJsonData(kafkaSourceDF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    kafkaSourceDF.select(
      from_json(col("value").cast("string"), invoiceSchema).alias("value")
    )
  }

// Function to process and write notification data to Kafka
 private def writeNotifications(valueDF: org.apache.spark.sql.DataFrame, conf: com.typesafe.config.Config): Unit = {
   // Extract the notification data
   val notificationDF = valueDF
      .select(
        "value.InvoiceNumber",
        "value.CustomerCardNo",
        "value.TotalAmount"
      )
      .withColumn("EarnedLoyaltyPoints", expr("TotalAmount*0.2"))

   // Convert the notification data to JSON
    val kafkaTargetDF = notificationDF.selectExpr(
      "InvoiceNumber as key",
      """to_json(named_struct('CustomerCardNo', CustomerCardNo,
        |'TotalAmount', TotalAmount,
        |'EarnedLoyaltyPoints', TotalAmount * 0.2
        |)) as value""".stripMargin
    )

   // Write the notification data to the Kafka topic
    kafkaTargetDF.writeStream
      .format("kafka")
      .queryName("Notification Writer")
      .option("kafka.bootstrap.servers", conf.getString("kafka.broker"))
      .option("topic", "notifications")
      .option("checkpointLocation", "chk-point-dir/notify")
      .outputMode("append")
      .start()
  }

// Function to explode, flatten, and write invoice data to JSON files
  private def writeFlattenedInvoices(valueDF: org.apache.spark.sql.DataFrame): Unit = {
    // Explode the invoice line items
    val explodeDF = valueDF.selectExpr(
      "value.InvoiceNumber",
      "value.CreatedTime",
      "value.StoreID",
      "value.PosID",
      "value.CustomerType",
      "value.PaymentMethod",
      "value.DeliveryType",
      "value.DeliveryAddress.City",
      "value.DeliveryAddress.State",
      "value.DeliveryAddress.PinCode",
      "explode(value.InvoiceLineItems) as LineItem"
    )

    // Flatten the invoice line items
    val flattenedDF = explodeDF
      .withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop("LineItem")

    // Write the flattened invoice data to a JSON file
    flattenedDF.writeStream
      .format("json")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "output")
      .option("checkpointLocation", "chk-point-dir/flatten")
      .start()
  }
}
