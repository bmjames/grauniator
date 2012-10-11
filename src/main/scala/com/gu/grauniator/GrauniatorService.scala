package com.gu.grauniator

import akka.dispatch.Future

import blueeyes.{BlueEyesServer, BlueEyesServiceBuilder}
import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.data.{BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http.MimeTypes.{text, plain}
import blueeyes.core.http.{HttpRequest, HttpResponse}
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser

import scalaz._, Scalaz._


trait GrauniatorService extends BlueEyesServiceBuilder
    with BijectionsChunkString
    with BijectionsChunkFutureJson {

  val grauniator = service("grauniator", "1.0.0") { context =>

    request {

      produce(text/plain) {

        accept(text/plain) { // Yes, SNS sends JSON as text/plain.
          path("/notify") {
            post { request: HttpRequest[Future[JValue]] =>

              for {
                notification <- request.content.sequence
              } yield {
                val message = extractMessage(notification)
                val words = ~(message map findMisspellings)
                HttpResponse[ByteChunk](status = OK, content = words.toNel map formatWords)
              }

            }
          }
        }

      }

    }

  }

  def extractMessage(notification: JValue): Option[JValue] =
    for {
      JString(message) <- notification \? "Message"
      json <- JsonParser.parseOpt(message)
    } yield json

  def findMisspellings(message: JValue): List[String] =
    for {
      JString(body) <- message \\ "body"
      word <- body.split("""\s+""").toList if commonMisspellings(word.toLowerCase)
    } yield word


  def formatWords(words: NonEmptyList[String]): String =
    words.toSet.mkString(", ") + "\n"

}

object Main extends BlueEyesServer with GrauniatorService
