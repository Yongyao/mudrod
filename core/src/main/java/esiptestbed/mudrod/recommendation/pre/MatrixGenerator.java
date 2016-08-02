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
package esiptestbed.mudrod.recommendation.pre;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.OBCodeExtractor;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatrixGenerator extends DiscoveryStepAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MatrixGenerator.class);

  public MatrixGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Metadata matrix starts******************");
    startTime = System.currentTimeMillis();

    String metadataCodeMatrixFile = config.get("metadataOBCodeMatrix");
    try {
      OBCodeExtractor extractor = new OBCodeExtractor();
      JavaPairRDD<String,Vector> metadataVecRDD = extractor.loadOBCode(this.es, this.spark.sc, config.get("indexName"),config.get("recom_metadataType"));
      RowMatrix wordDocMatrix = new RowMatrix(metadataVecRDD.values().rdd());
      List<String> rowKeys = metadataVecRDD.keys().collect();
      MatrixUtil.exportToCSV(wordDocMatrix, rowKeys, null,metadataCodeMatrixFile);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************Metadata matrix ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
