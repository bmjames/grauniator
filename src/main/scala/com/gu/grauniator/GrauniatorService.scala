package com.gu.grauniator

import com.gu.grauniator.aws.BlueeyesSNS

import akka.dispatch.{ExecutionContext, Future}
import blueeyes.{BlueEyesServer, BlueEyesServiceBuilder}
import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.data.{BijectionsChunkFutureJson, BijectionsChunkString}
import blueeyes.core.http.MimeTypes.{text, plain}
import blueeyes.core.http.{HttpRequest, HttpResponse}
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.service.ServiceContext
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz._, Scalaz._


trait GrauniatorService extends BlueEyesServiceBuilder
    with BijectionsChunkString
    with BijectionsChunkFutureJson {

  val grauniator = service("grauniator", "1.0.0") { context =>
    startup {
      Future(GrauniatorConfig(context))
    } ->
    request { case GrauniatorConfig(sns, topicArn) =>

      produce(text/plain) {

        accept(text/plain) { // Yes, SNS sends JSON as text/plain.
          path("/notify") {
            post { request: HttpRequest[Future[JValue]] =>

              implicit val m = sns.M
              for {
                notification <- request.content.sequence
                message = extractMessage(notification)
                words = ~(message map findMisspellings)
                url = message flatMap extractUrl
                alertMessage = (url |@| words.toNel)(formatMessage)
                _ <- alertMessage traverse (msg => sns.publish("Grauniator Alert", topicArn, msg))
              } yield {
                HttpResponse[String](status = OK, content = alertMessage)
              }

            }
          }
        }

      }

    } ->
    shutdown(_ => Future(()))
  }

  def extractMessage(notification: JValue): Option[JValue] =
    for {
      JString(message) <- notification \? "Message"
      json <- JsonParser.parseOpt(message)
    } yield json

  def extractUrl(message: JValue): Option[String] =
    (for {
      JString(url) <- (message \\ "webUrl")
    } yield url).headOption

  def findMisspellings(content: JValue): List[String] =
    for {
      JString(body) <- content \\ "body"
      word <- body.split("""\s+""").toList if commonMisspellings(word.toLowerCase)
    } yield word

  def formatMessage(url: String, words: NonEmptyList[String]): String =
    words.toSet.mkString(", ") + " | " + url + "\n"

}

object Main extends BlueEyesServer with GrauniatorService

case class GrauniatorConfig(sns: BlueeyesSNS, topicArn: String)

object GrauniatorConfig {
  def apply(context: ServiceContext)(implicit executor: ExecutionContext): GrauniatorConfig = {
    val awsId = context.config[String]("awsId")
    val awsKey = context.config[String]("awsKey")
    val endpoint = context.config[String]("endpoint")
    val topicArn = context.config[String]("topicArn")
    GrauniatorConfig(new BlueeyesSNS(awsId, awsKey, endpoint), topicArn)
  }
}
