/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nasa.jpl.mudrod.ssearch;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.ssearch.structure.SResult;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends MudrodAbstract implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  DecimalFormat NDForm = new DecimalFormat("#.##");
  final Integer MAX_CHAR = 700;

  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of converting processing level string into a number
   *
   * @param pro processing level string
   * @return processing level number
   */
  public Double getProLevelNum(String pro) {
    if (pro == null) {
      return 1.0;
    }
    Double proNum;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if (pro.matches("[0-9]{1}[a-zA-Z]{1}")) {
      proNum = Double.parseDouble(pro.substring(0, 1));
    } else if (p.matcher(pro).find()) {
      proNum = 1.0;
    } else {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  public Double getPop(Double pop) {
    if (pop > 1000) {
      pop = 1000.0;
    }
    return pop;
  }

  /**
   * Method of checking if query exists in a certain attribute
   *
   * @param strList attribute value in the form of ArrayList
   * @param query   query string
   * @return 1 means query exists, 0 otherwise
   */
  public Double exists(ArrayList<String> strList, String query) {
    Double val = 0.0;
    if (strList != null) {
      String str = String.join(", ", strList);
      if (str != null && str.length() != 0 && str.toLowerCase().trim().contains(query)) {
        val = 1.0;
      }
    }
    return val;
  }

  /**
   * Main method of semantic search
   *
   * @param index          index name in Elasticsearch
   * @param type           type name in Elasticsearch
   * @param query          regular query string
   * @param queryOperator query mode- query, or, and
   * @return a list of search result
   */
  @SuppressWarnings("unchecked")
  public List<SResult> searchByQuery(String index, String type, String query, String queryOperator, String rankOption) {
    boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return new ArrayList<>();
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0, queryOperator);
    List<SResult> resultList = new ArrayList<>();

    SearchRequestBuilder builder = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb).setSize(500).setTrackScores(true);
    SearchResponse response = builder.execute().actionGet();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      
      String shortName = (String) ((Map<String, Object>) result.get("global_attributes")).get("Model");
      String content = (String) ((Map<String, Object>) result.get("global_attributes")).get("History");
      String longName = "Dust storm data " + shortName;

      SResult re = new SResult(shortName, longName, "Dust storm data", content, "");

      resultList.add(re);
    }

    return resultList;
  }

  /**
   * Method of semantic search to generate JSON string
   *
   * @param index          index name in Elasticsearch
   * @param type           type name in Elasticsearch
   * @param query          regular query string
   * @param queryOperator query mode- query, or, and
   * @param rr             selected ranking method
   * @return search results
   */
  public String ssearch(String index, String type, String query, String queryOperator, String rankOption, Ranker rr) {
    List<SResult> li = searchByQuery(index, type, query, queryOperator, rankOption);
//    if ("Rank-SVM".equals(rankOption)) {
//      li = rr.rank(li);
//    }
    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<>();

    for (int i = 0; i < li.size(); i++) {
      JsonObject file = new JsonObject();
      file.addProperty("Short Name", (String) SResult.get(li.get(i), "shortName"));
      file.addProperty("Long Name", (String) SResult.get(li.get(i), "longName"));
      file.addProperty("Topic", (String) SResult.get(li.get(i), "topic"));
      file.addProperty("Description", (String) SResult.get(li.get(i), "description"));
//      file.addProperty("Release Date", (String) SResult.get(li.get(i), "relase_date"));
      fileList.add(file);
//
//      file.addProperty("Start/End Date", (String) SResult.get(li.get(i), "startDate") + " - " + (String) SResult.get(li.get(i), "endDate"));
//      file.addProperty("Processing Level", (String) SResult.get(li.get(i), "processingLevel"));
//
//      file.addProperty("Sensor", (String) SResult.get(li.get(i), "sensors"));
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject pDResults = new JsonObject();
    pDResults.add("PDResults", fileListElement);
    return pDResults.toString();
  }
}
