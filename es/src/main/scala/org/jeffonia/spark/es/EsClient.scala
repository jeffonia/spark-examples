package org.jeffonia.spark.es

import java.net.InetAddress

import com.google.gson.JsonObject
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.search.sort.{SortOrder, SortParseElement}
import org.jeffonia.spark.common.JsonUtils

/**
  * Created by gjf11847 on 2017/8/24.
  */
class EsClient {

}

object EsClient {
  lazy val esConf: JsonObject = JsonUtils.load("es_conf.json").getAsJsonObject

  lazy val esTransport: TransportClient = {
    val settings = Settings.settingsBuilder()
      .put("client.transport.sniff", esConf.get("client.transport.sniff"))
      .put("cluster.name", esConf.get("cluster.name"))
      .build
    val client = TransportClient.builder().settings(settings).build
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.100.157.99"), 9700))
    println("*****************************" + client)
    client
  }

  def transportClient(host: String, cluster: String, port: Int, transportClient: TransportClient): TransportClient = {
    def getInstance = {
      val settings = Settings.settingsBuilder()
        .put("client.transport.sniff", "true")
        .put("cluster.name", cluster)
        .build
      TransportClient.builder().settings(settings).build
    }

    println("*****************************" + transportClient)

    val client: TransportClient = if (null != transportClient) transportClient else getInstance
    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
  }

  //  val getAllList: Unit = {
  //    val encode = DatatypeConverter.printBase64Binary("sy16420:clustermonitor".getBytes(Charsets.UTF_8))
  //    val response: SearchResponse = esTransport.prepareSearch("premier")
  //      .putHeader("searchguard_transport_creds", encode)
  //      .setTypes("loginfo")
  //      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
  //      .setQuery(QueryBuilders.boolQuery.must(QueryBuilders.matchAllQuery))
  //      .addSort("date", SortOrder.DESC)
  //      .setFrom(0)
  //      .setSize(100)
  //      .execute
  //      .actionGet
  //    val hits = response.getHits
  //    for (searchHit <- hits.getHits) println(searchHit.getSourceAsString)
  //  }

  def readFromEs(esTransport: TransportClient, json: String, indices: String, types: String): Unit = {
    val searchResponse = esTransport.prepareSearch(indices)
      .setTypes(types)
      .setSource(json)
      .setSize(ConstEs.SIZE_OF_RESULT)
      .execute
      .actionGet
    searchResponse.getHits
    val hits = searchResponse.getHits
    //    for (searchHit <- hits.getHits) println(searchHit.getSourceAsString)
    //    val hits = searchResponse.getHits
    //    for (searchHit <- hits.getHits) {
    //      searchHit.getFields.remove()
    //    }
  }

  /**
    *
    * @param indices indices.
    * @param query   query of es.
    * @tparam T One of String and TermQueryBuilder type.
    */
  def readFromEs[T](indices: String, query: T): Unit = {
    val searchRequestBuilder = esTransport.prepareSearch(indices)
      .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
      .setScroll(TimeValue.timeValueMillis(ConstEs.TIME_SCROLL_ALIVE_MS))
      .setSize(ConstEs.SIZE_OF_RESULT) //100 hits per shard will be returned for each scroll

    query match {
      case _: String => searchRequestBuilder.setSource(new JSONObject(query.asInstanceOf[String]).toString)
      case _: TermQueryBuilder => searchRequestBuilder.setQuery(query.asInstanceOf[TermQueryBuilder])
      case _ =>
    }

    var scrollResp = searchRequestBuilder.execute.actionGet
    //Scroll until no hits are returned
    while (scrollResp.getHits.getHits.length > 0) {
      for (searchHit <- scrollResp.getHits.getHits) {
        println(searchHit)
      }
      scrollResp = esTransport.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(TimeValue.timeValueMillis(ConstEs.TIME_SCROLL_ALIVE_MS))
        .execute
        .actionGet
    }
  }

  def writeToEs[T](index: String, `type`: String, data: List[Map[String, T]]): BulkResponse = {
    val bulkRequest = esTransport.prepareBulk()
    data.foreach(line => {
      val xContentBuilder = XContentFactory.jsonBuilder().startObject()
      line.foreach(field => xContentBuilder.field(field._1, field._2))
      val request = esTransport.prepareIndex(index, `type`).setSource(xContentBuilder.endObject())
      bulkRequest.setTimeout(TimeValue.timeValueSeconds(ConstEs.TIME_OUT_MS)).add(request)
    })
    bulkRequest.execute().actionGet()
  }

}

object ConstEs {
  val TIME_SCROLL_ALIVE_MS = 60000L
  val TIME_OUT_MS = 60000L
  val SIZE_OF_RESULT = 100
}