package org.hotosm.oam

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._

import org.apache.commons.io._
import com.typesafe.scalalogging.slf4j._
import java.io.{InputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream }
import scala.collection.JavaConverters._
import scala.collection.mutable

class AWSClient(credentials: AWSCredentials, config: ClientConfiguration) {
  val s3client = new com.amazonaws.services.s3.AmazonS3Client(credentials, config)

  def readTextFile(uri: String): String = {
    val parsed = new java.net.URI(uri)
    val bucket = parsed.getHost
    val key = parsed.getPath.drop(1)
    val obj = s3client.getObject(new GetObjectRequest(bucket, key))
    val contentStream = obj.getObjectContent
    IOUtils.toString(contentStream)
  }

  // def getFileKeys(listing: ObjectListing): Seq[String] =
  //   listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")

  def listKeys(bucket: String, key: String): Seq[String] = {
    val request = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(key)
    
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3client.listObjects(request)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      request.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result.toSeq
    // val request = new ListObjectsRequest(bucket, key, null, null, null)
    // new Iterator[String] {
    //   var listing = s3client.listObjects(request)
    //   var iter = getFileKeys(listing).iterator

    //   def getNextPage: Boolean =  {
    //     val nextRequest = request.withMarker(listing.getNextMarker)
    //     listing = s3client.listObjects(nextRequest)
    //     iter = getFileKeys(listing).iterator
    //     iter.hasNext
    //   }

    //   def hasNext: Boolean = {
    //     iter.hasNext || getNextPage
    //   }

    //   def next: String = iter.next
    // }
  }

  private def readInputStream(inStream: InputStream): Array[Byte] = {
    val bufferSize = 0x20000
    val buffer = new Array[Byte](bufferSize)
    val outStream = new ByteArrayOutputStream(bufferSize)
    var bytes: Int = 0
    while (bytes != -1) {
      bytes = inStream.read(buffer)
      if (bytes != -1) outStream.write(buffer, 0, bytes);
    }
    outStream.toByteArray
  }

  def readBytes(bucket: String, key: String): Array[Byte] = {
    val obj = s3client.getObject(new GetObjectRequest(bucket, key))
    val inStream = obj.getObjectContent
    try {
      readInputStream(inStream)
    } finally {
      inStream.close()
    }
  }

  def putBytes(bucket: String, key: String, bytes: Array[Byte]): Unit = {
    val metadata = new ObjectMetadata()
    metadata.setContentLength(bytes.length)
    val stream = new ByteArrayInputStream(bytes)
    val request = new PutObjectRequest(bucket, key, stream, metadata)
    s3client.putObject(request)
  }
}

object AWSClient {
  def default = {
    val provider = new DefaultAWSCredentialsProviderChain()
    val config = new com.amazonaws.ClientConfiguration
    config.setMaxConnections(128)
    config.setMaxErrorRetry(16)
    config.setConnectionTimeout(100000)
    config.setSocketTimeout(100000)
    config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
    new AWSClient(provider.getCredentials, config)
  }
}
