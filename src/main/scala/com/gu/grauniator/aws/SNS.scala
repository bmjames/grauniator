package com.gu.grauniator
package aws

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat

import scalaz.Monad


/*
 * Credit to https://gist.github.com/633175
 */
abstract class SNS(awsId: String, awsKey: String, endpoint: String) {
  import SNS._

  type F[_]

  def M: Monad[F]

  type Result

  def client: Client

  trait Client {
    def post(path: String)(content: String): F[Result]
  }

  def publish(subject: String, topicArn: String, message: String): F[Result] = {
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
