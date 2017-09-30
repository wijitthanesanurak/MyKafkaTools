package com.trueit.tools

import kafka.consumer.SimpleConsumer
import kafka.common.TopicAndPartition
import kafka.api._
import scala.annotation.tailrec
import scala.util._
import kafka.common.KafkaException

object KafkaSimpleConsumerUtils {
  def getLastOffset(consumer: SimpleConsumer, topic: String, partition: Int, whichTime: Long, clientName: String): Try[Long] = {
    val tap = new TopicAndPartition(topic, partition)
    val request = new kafka.api.OffsetRequest(Map(tap -> PartitionOffsetRequestInfo(whichTime, 1)))
    val response = consumer.getOffsetsBefore(request)

    if (response.hasError) {
      val err = response.partitionErrorAndOffsets(tap).error
      Failure(new KafkaException("Error fetching data Offset Data the Broker. Reason: " + err))
    } else {
      //offsets is sorted in descending order, we always want the first
      Success(response.partitionErrorAndOffsets(tap).offsets(0))
    }
  }
  
  def main (args: Array[String]) {
    val simpleConsumer = new SimpleConsumer("127.0.0.1", 9092, 100000, 64 * 1024, "leaderLookup");
		val lastOffset = getLastOffset(simpleConsumer, "t1", 0, -2, "test")
		System.out.println(lastOffset)
  }
}