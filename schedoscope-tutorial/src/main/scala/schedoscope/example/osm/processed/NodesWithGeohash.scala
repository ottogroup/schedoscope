/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
package schedoscope.example.osm.processed

import org.schedoscope.dsl.View
import org.schedoscope.dsl.TextFile
import org.schedoscope.dsl.transformations.MapreduceTransformation
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.Path
import schedoscope.example.osm.mapreduce.GeohashMapper

case class NodesWithGeohash() extends View {
  val id = fieldOf[Long]
  val version = fieldOf[Int]
  val user_id = fieldOf[Int]
  val tstamp = fieldOf[String]
  val longitude = fieldOf[Double]
  val latitude = fieldOf[Double]
  val geohash = fieldOf[String]

  val stageNodes = dependsOn { () => schedoscope.example.osm.stage.Nodes() }

  transformVia(() =>
    MapreduceTransformation(
      this,
      (conf: Map[String, Any]) => {
        val job = Job.getInstance
        LazyOutputFormat.setOutputFormatClass(job, classOf[TextOutputFormat[Text, NullWritable]]);
        job.setJobName(this.urlPath)
        job.setJarByClass(classOf[GeohashMapper])
        job.setMapperClass(classOf[GeohashMapper])
        job.setNumReduceTasks(0)
        FileInputFormat.setInputPaths(job, conf.get("input_path").get.toString);
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output_path").get.toString));
        val cfg = job.getConfiguration();
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
          cfg.set("mapreduce.job.credentials.binary",
            System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        job
      }).configureWith(
        Map(
          "input_path" -> stageNodes().fullPath,
          "output_path" -> fullPath)))

  comment("nodes, extended with geohash")

  storedAs(TextFile(fieldTerminator = "\\t", lineTerminator = "\\n"))
}