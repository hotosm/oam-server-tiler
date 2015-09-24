package org.hotosm.oam

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._
import com.amazonaws.regions.Regions

import org.apache.commons.io._
import com.typesafe.config._
import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream }
import scala.collection.JavaConverters._
import scala.collection.mutable

object Status {
  lazy val queueUrl = {
    val config = ConfigFactory.load()
    config.getString("oam.statusSqsQueueUrl")
  }

  lazy val sqsRegion = {
    val config = ConfigFactory.load()
    config.getString("oam.statusSqsRegion")
  }

  def sendNotification(msg: String): Unit = {
    val client = new AmazonSQSClient()
    val req = new SendMessageRequest(queueUrl, msg)
    val result = client.sendMessage(req)
    println("MessageId - " + result.getMessageId);
  }

  def notifyStart(jobId: String): Unit =
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "STARTED" }""")

  def notifyFailure(jobId: String, exception: Exception): Unit =
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "FAILED", "error": "$exception" }""")

  def notifySuccess(jobId: String, target: String, sourceUris: Seq[String]): Unit = {
    val csv = sourceUris.map { uri => s""""$uri"""" }.mkString(",")
    val sourceUrisJson = s"[$csv]"
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "FINISHED", "target": "$target", "images": $sourceUrisJson }""")
  }
}
