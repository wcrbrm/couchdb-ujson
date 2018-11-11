import ammonite.ops._
val wd = pwd
import $ivy.`org.scalaj::scalaj-http:2.4.1`
import $ivy.`com.lihaoyi::ujson:0.6.6`, ujson._
import scalaj.http.{Http, HttpResponse}
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

case class CouchDatabase (
    database: String, 
    host: String = "localhost", 
    port:Int = 5984, 
    protocol: String = "http"
) {

  def realDatabase: String = database.replace("-", "_")
  def root: String = protocol + "://" + host + ":"  + port + "/" + realDatabase
  def bulkDocs: String = root + "/_bulk_docs"

  def post(url: String, payload: String): HttpResponse[String] = {
    Http(url).header ("Content-Type", "application/json").postData(payload).asString
  }
  def bulkPost(docs: Js.Value) = {
    println( Http(bulkDocs).header("Content-Type", "application/json")
      .postData(docs.toString).asString.body )
  }
}

object UrlPart {
  def quote(x:String): String = "\"" + x + "\""
  def encode(x:String): String = java.net.URLEncoder.encode(quote(x), "utf-8")
}

trait CouchUrlFilter {
  def asUrlPart: String
}
case class CouchUrlFilterExact(key: String) extends CouchUrlFilter {
  override def asUrlPart: String = "key=" + UrlPart.encode(key)
}
case class CouchUrlFilterBetween(
    startKey: Option[String] = None, 
    endKey: Option[String] = None
) extends CouchUrlFilter {

  override def asUrlPart: String = {
    val params = new ListBuffer[String]
    startKey.map(k => { params += "startkey=" + UrlPart.encode(k) })
    endKey.map(k => { params += "endkey=" + UrlPart.encode(k) })
    params.mkString("&")
  }
}
case class CouchUrlFilterStartingWith(
    key: String, 
    descending: Boolean = false
) extends CouchUrlFilter {

  override def asUrlPart: String = {
    if (descending) {
      List("startkey=[" + UrlPart.encode(key) + ",{}]", "endkey=[" + UrlPart.encode(key) + "]").mkString("&")
    } else {
      List("startkey=[" + UrlPart.encode(key) + "]", "endkey=[" + UrlPart.encode(key) + ",{}]").mkString("&")
    }
  }
}
case class CouchUrlIntFilterStartingWith(
    key: Int, 
    descending: Boolean = false
) extends CouchUrlFilter {

  override def asUrlPart: String = {
    if (descending) {
      List(
        "startkey=[" + key + ",{}]", 
        "endkey=[" + key + "]"
      ).mkString("&")
    } else {
      List(
        "startkey=[" + key + "]", 
        "endkey=[" + key + ",{}]"
      ).mkString("&")
    }
  }
}

case class CouchBatch(limit: Int = 500, var skip: Int = 0) {
  def asUrlPart: String = "limit=" + limit + "&skip=" + skip
  def next = { skip += limit }
}

object DefaultBatch extends CouchBatch()
object TinyBatch extends CouchBatch(limit = 5)
object SmallBatch extends CouchBatch(limit = 100)

case class CouchUrlBuilder(
    basePath: String,
    includeDocs: Boolean = false,
    reduce: Boolean = false,
    descending: Option[Boolean] = None,
    group: Option[String] = None,
    filter: Option[CouchUrlFilter] = None,
    batch: CouchBatch = DefaultBatch,
    stale: Boolean = false
) {
  def getUrl: String = {
    val params = new ListBuffer[String]
    params += "reduce=" + (if (reduce) "true" else "false")
    params += "include_docs=" + (if (includeDocs) "true" else "false")
    group.map(g => { params += "group=" + g })
    filter.map(f => { params += f.asUrlPart })
    descending.map(d => { params += "descending=" + (if (d) "true" else "false") })
    if (stale) { params += "stale=ok"; }
    params += batch.asUrlPart
    basePath + "?" + params.mkString("&")
  }
}

case class CouchView(db: CouchDatabase, urlBuilder: CouchUrlBuilder) {

  def getUrl: String = db.root + urlBuilder.getUrl
  def get: HttpResponse[String] = Http(getUrl).asString

  def docs(parser: Js.Obj => Unit): Unit = {
    if (urlBuilder.includeDocs) {
      val Js.Obj(body) = ujson.read(get.body)
      val rows = body.get("rows").get.arr
      for {
        row <- rows
        doc <- row.obj.get("doc")
      } yield parser(doc.obj)
    } else {
      throw new Exception("includeDocs must be true for scanning documents")
    }
  }

  def allDocs[T](parser: (Js.Obj) => T): Any = {
    if (urlBuilder.includeDocs) {
      var canContinue: Boolean = true
      do {
        println("Reading Batch: " + getUrl)

        val Js.Obj(body) = ujson.read(get.body)
        val rows = body.get("rows").get.arr
        canContinue = rows.length == urlBuilder.batch.limit
        for {
          row <- rows
          doc <- row.obj.get("doc")
        } yield parser(doc.obj)

        urlBuilder.batch.next
      } while (canContinue)
    } else {
      throw new Exception("includeDocs must be true for scanning documents")
    }
  }

}
