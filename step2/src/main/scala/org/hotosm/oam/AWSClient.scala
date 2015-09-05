package org.hotosm.oam

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.ClientConfiguration
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._


import org.apache.commons.io._
import com.typesafe.scalalogging.slf4j._

class AWSClient(credentials: AWSCredentials, config: ClientConfiguration) {
  private val s3client = new com.amazonaws.services.s3.AmazonS3Client(credentials, config)

  def readTextFile(uri: String): String = {
    val parsed = new java.net.URI(uri)
    val bucket = parsed.getHost
    val key = parsed.getPath.drop(1)
    val obj = s3client.getObject(new GetObjectRequest(bucket, key))
    val contentStream = obj.getObjectContent
    IOUtils.toString(contentStream)
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
