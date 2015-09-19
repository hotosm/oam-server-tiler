package org.hotosm.oam

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sns.AmazonSNSClient
import com.amazonaws.services.sns.model._
import com.amazonaws.regions.Regions

import org.apache.commons.io._
import com.typesafe.config._
import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream }
import scala.collection.JavaConverters._
import scala.collection.mutable

object SnsClient {
  lazy val topicArn = {
    val config = ConfigFactory.load()
    config.getString("oam.statusSnsArn")
  }

  lazy val snsRegion = {
    val config = ConfigFactory.load()
    config.getString("oam.statusSnsRegion")
  }

  def sendNotification(msg:String): Unit = {
    val snsClient = new AmazonSNSClient()
    snsClient.setRegion(Regions.fromName(snsRegion))
    val req = new PublishRequest(topicArn, msg)
    val result = snsClient.publish(req)
    println("MessageId - " + result.getMessageId);
  }

  def notifyStart(jobId: String): Unit =
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "STARTED" }""")

  def notifyFailure(jobId: String, exception: Exception): Unit =
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "FAILED", "error": "$exception" }""")

  def notifySuccess(jobId: String): Unit =
    sendNotification(s"""{ "jobId": "$jobId", "stage": "mosaic", "status": "FINISHED" }""")
}
