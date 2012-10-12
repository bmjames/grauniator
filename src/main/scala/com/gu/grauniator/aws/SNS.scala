package com.gu.grauniator
package aws

import akka.dispatch.Future
import blueeyes.core.data.ByteChunk
import blueeyes.core.http.{URI, HttpRequest, HttpResponse}
import blueeyes.core.service.engines.HttpClientXLightWeb
import scalaz._, Scalaz._

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import blueeyes.core.http.HttpMethods.GET


/*
 * Credit to https://gist.github.com/633175
 */
final class SNS(awsId: String, awsKey: String, endpoint: String) {

  import SNS._
  import blueeyes.core.data.BijectionsChunkString._

  val client = new HttpClientXLightWeb().path("http://" + endpoint).translate[String]

  def publish(subject: String, topicArn: String, message: String): Future[HttpResponse[String]] = {
    val params = List(
      "Subject" -> subject,
      "TopicArn" -> topicArn,
      "Message" -> message,
      "Timestamp" -> formatDateTime(new DateTime),
      "AWSAccessKeyId" -> awsId,
      "Action" -> "Publish",
      "SignatureVersion" -> "2",
      "SignatureMethod" -> algorithm
    )
    val signature = signRequest(params)
    val signedParams = ("Signature" -> signature) :: params
    val path = "/?" + canonicalQueryString(signedParams)
    client.get[String](path)
  }

  private def signRequest(params: List[(String, String)]): String = {
    val hmac = Mac.getInstance(algorithm)
    hmac.init(secretKey)
    val stringToSign = List("GET", endpoint, "/", canonicalQueryString(params)).mkString("\n")
    val bytes = hmac.doFinal(stringToSign.getBytes(charset))
    Base64.encodeBase64String(bytes)
  }

  private def secretKey: SecretKeySpec = new SecretKeySpec(awsKey.getBytes, algorithm)

}

object SNS {

  def canonicalQueryString(params: List[(String, String)]): String =
    params.sortBy(_._1) map { case (k, v) => urlEncode(k) + "=" + urlEncode(v) } mkString "&"

  def formatDateTime(dt: DateTime): String =
    ISODateTimeFormat.dateTime.withZone(DateTimeZone.UTC).print(dt)

  val charset = "utf-8"

  val algorithm = "HmacSHA256"

  def urlEncode(s: String): String = java.net.URLEncoder.encode(s, "utf-8").replaceAllLiterally("+", "%20")
  
}
