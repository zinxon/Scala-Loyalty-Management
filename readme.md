# Kafka Stream Invoices Processing with Apache Spark (Docker)

This Spark Streaming application processes retail invoice data from Kafka. 
It reads invoices, calculates customer loyalty points (0.2 points per dollar spent), 
and sends notifications to another Kafka topic. The program enables real-time sales tracking, 
loyalty program management, and automated customer reward calculations, streamlining retail operations and enhancing customer engagement.

## Project Structure

- `src/main/scala/KafkaIOStream.scala`: Main Scala application for Kafka stream processing for Input Stream & Output Stream
- `docker-compose.yml`: Docker Compose file for setting up Kafka and Zookeeper
- `build.sbt`: SBT build configuration file
- `samples.json`: Sample Invoice Data 

## Prerequisites

- Java 8 or higher
- Scala 2.13.15
- Apache Spark 3.5.0
- Docker and Docker Compose

## Setup

1. Start the Kafka and Zookeeper containers:
   docker-compose up -d

2. Create the Kafka topic (invoices & notifications):
   ```
   docker exec -it kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic invoices
   docker exec -it kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic notifications
   ```

3. List Kafka Topics to confirm topic created:
   ```
   docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
   ```
   It will return:
   ```
   invoices
   notifications
   ```

## Running the Application

1. Build the project:

   ```
   sbt compile
   ```

2. Run the Spark application:

   ```
   sbt run
   ```

3. To produce messages to the invoices topic:

   ```
   docker exec -it kafka kafka-console-producer --broker-list localhost:9092 --topic invoices
   ```
   
4. To consume messages from notifications topic:   
   ```
   docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic notifications
   ```
  
5. Send Invoice data to invoices to invoices topic
   ```
   {"InvoiceNumber":"51402977","CreatedTime":1595688900348,"StoreID":"STR7188","PosID":"POS956","CashierID":"OAS134","CustomerType":"PRIME","CustomerCardNo":"4629185211","TotalAmount":11114.0,"NumberOfItems":4,"PaymentMethod":"CARD","TaxableAmount":11114.0,"CGST":277.85,"SGST":277.85,"CESS":13.8925,"DeliveryType":"TAKEAWAY","InvoiceLineItems":[{"ItemCode":"458","ItemDescription":"Wine glass","ItemPrice":1644.0,"ItemQty":2,"TotalValue":3288.0},{"ItemCode":"283","ItemDescription":"Portable Lamps","ItemPrice":2236.0,"ItemQty":1,"TotalValue":2236.0},{"ItemCode":"498","ItemDescription":"Carving knifes","ItemPrice":1424.0,"ItemQty":2,"TotalValue":2848.0},{"ItemCode":"523","ItemDescription":"Oil-lamp clock","ItemPrice":1371.0,"ItemQty":2,"TotalValue":2742.0}]}
   ```

## Application Details

The main application (`KafkaStream.scala`) does the following:

1. Sets up a Spark session
2. Defines a schema for the incoming JSON data
3. Reads from the Kafka topic "invoices"
4. Processes the data (flattens the nested structure)
5. Writes the processed data to Kafka output stream "notifications"

## Configuration

- Kafka bootstrap servers: `kafka:9092` (internal Docker network)
- Kafka topic: `invoices`,`notifications`
- Checkpoint location: `chk-point-dir/`

## Dependencies

- Apache Spark Core 3.5.0
- Apache Spark SQL 3.5.0
- Apache Spark SQL Kafka 0-10 3.5.0

For a complete list of dependencies, refer to the `build.sbt` file.

## Notes

- Ensure that the Kafka service is running and accessible before starting the Spark application.
- The application is configured to run in local mode. For production, adjust the Spark master configuration accordingly.
