package com.gu.grauniator.aws

import akka.dispatch.{Future, ExecutionContext}
import blueeyes.core.data._
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpHeaders.`Content-Type`
import blueeyes.core.http.MimeTypes.{application, `x-www-form-urlencoded`}
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.core.service.HttpClient

import scalaz.Monad
import scalaz.Scalaz._


final class BlueeyesSNS(awsId: String, awsKey: String, endpoint: String)(implicit executor: ExecutionContext)
  extends SNS(awsId, awsKey, endpoint) {

  type F[a] = Future[HttpResponse[a]]

  type Result = ByteChunk

  val M = new Monad[F] {
    def point[A](a: => A) = Future(HttpResponse(content = Some(a)))
    def bind[A, B](fa: Future[HttpResponse[A]])(f: A => Future[HttpResponse[B]]) =
      fa flatMap { response =>
        response.content map f getOrElse Future(response.copy(content = none[B]))
      }
  }

  import blueeyes.core.data.BijectionsChunkString._

  val client = new Client {
    val c: HttpClient[ByteChunk] =
      new HttpClientXLightWeb()
        .path("http://" + endpoint)
        .header(`Content-Type`(application/`x-www-form-urlencoded`))
    def post(path: String)(content: String) = c.post(path)(content)
  }

}
