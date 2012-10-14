package com.gu.grauniator
package aws

import akka.dispatch.Future
import blueeyes.core.data.ByteChunk
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpHeaders.`Content-Type`
import blueeyes.core.http.MimeTypes.{application, `x-www-form-urlencoded`}
import blueeyes.core.service.engines.HttpClientXLightWeb
import scalaz._, Scalaz._

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import blueeyes.core.service.HttpClient


/*
 * Credit to https://gist.github.com/633175
 */
final class SNS(awsId: String, awsKey: String, endpoint: String) {

  import SNS._
  import blueeyes.core.data.BijectionsChunkString._

  val client: HttpClient[ByteChunk] =
    new HttpClientXLightWeb()
      .path("http://" + endpoint)
      .header(`Content-Type`(application/`x-www-form-urlencoded`))

  def publish(subject: String, topicArn: String, message: String): Future[HttpResponse[ByteChunk]] = {
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
    val signature = signRequest("POST", "/", params)
    val signedParams = ("Signature" -> signature) :: params
    client.post("/")(canonicalQueryString(signedParams))
  }

  private def signRequest(method: String, path: String, params: List[(String, String)]): String = {
    val hmac = Mac.getInstance(algorithm)
    hmac.init(secretKey)
    val stringToSign = List(method, endpoint, path, canonicalQueryString(params)).mkString("\n")
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

  def urlEncode(s: String): String = java.net.URLEncoder.encode(s, charset).replaceAllLiterally("+", "%20")
  
}
