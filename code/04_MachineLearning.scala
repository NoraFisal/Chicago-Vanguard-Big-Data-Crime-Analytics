Windows PowerShell
Copyright (C) Microsoft Corporation. All rights reserved.

Install the latest PowerShell for new features and improvements! https://aka.ms/PSWindows

PS C:\Users\ragha> spark-shell
Unable to get Charset 'cp720' for property 'sun.stderr.encoding', using default windows-1256 and continuing.
Unable to get Charset 'cp720' for property 'sun.stderr.encoding', using default windows-1256 and continuing.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/05/09 20:17:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/05/09 20:17:23 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark context Web UI available at http://192.168.100.22:4041
Spark context available as 'sc' (master = local[*], app id = local-1778347043506).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.8
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.30)
Type in expressions to have them evaluated.
Type :help for more information.

scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

scala> import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.Pipeline

scala> import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeans

scala> import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.evaluation.ClusteringEvaluator

scala> val rawDF=spark.read.option("header","true").option("inferSchema","true").csv("file:/D:/BigData/phase2Data/transformed_data.csv")
[Stage 1:>                                                          (0 + 8) [Stage 1:=======>                                                   (1 + 7) [Stage 1:===================================================>       (7 + 1)                                                                     rawDF: org.apache.spark.sql.DataFrame = [Primary Type: string, Location Description: string ... 23 more fields]

scala> println("Rows: "+rawDF.count()+" | Columns: "+rawDF.columns.length)
Rows: 419883 | Columns: 25

scala> val featureCols=Array("Latitude","Longitude","Hour","Location_Crime_Count","PrimaryType_ArrestRate")
featureCols: Array[String] = Array(Latitude, Longitude, Hour, Location_Crime_Count, PrimaryType_ArrestRate)

scala> val contextCols=Seq("Primary Type","District","Time_Of_Day","Is_Night","Is_Weekend")
contextCols: Seq[String] = List(Primary Type, District, Time_Of_Day, Is_Night, Is_Weekend)

scala> var df=rawDF.select((featureCols.map(col)++contextCols.map(col)):_*)
df: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 8 more fields]

scala> df=df.na.drop(featureCols)
df: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 8 more fields]

scala> df=df.na.fill("unknown",Seq("Primary Type","Time_Of_Day"))
df: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 8 more fields]

scala> df=df.na.fill(0,Seq("District","Is_Night","Is_Weekend"))
df: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 8 more fields]

scala> featureCols.foreach(c=>df=df.withColumn(c,col(c).cast("double")))

scala> df.select(featureCols.map(col):_*).describe().show(false)
26/05/09 20:21:51 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
[Stage 5:>                                                          (0 + 8) [Stage 5:=======>                                                   (1 + 7)                                                                             +-------+-------------------+--------------------+------------------+--------------------+----------------------+
|summary|Latitude           |Longitude           |Hour              |Location_Crime_Count|PrimaryType_ArrestRate|
+-------+-------------------+--------------------+------------------+--------------------+----------------------+
|count  |419883             |419883              |419883            |419883              |419883                |
|mean   |41.842406956030715 |-87.67126385690644  |13.098744173972273|51370.25636427291   |0.25064839491003005   |
|stddev |0.08980122030383618|0.061979081995622555|6.743998705549545 |41702.99595999077   |0.2832795897673914    |
|min    |36.619446395       |-91.686565684       |0.0               |1.0                 |0.0                   |
|max    |42.022642635       |-87.524529465       |23.0              |110079.0            |1.0                   |
+-------+-------------------+--------------------+------------------+--------------------+----------------------+


scala> val assembler=new VectorAssembler().setInputCols(featureCols).setOutputCol("features_raw").setHandleInvalid("skip")
assembler: org.apache.spark.ml.feature.VectorAssembler = VectorAssembler: uid=vecAssembler_095f6b750920, handleInvalid=skip, numInputCols=5

scala> val scaler=new StandardScaler().setInputCol("features_raw").setOutputCol("features").setWithMean(true).setWithStd(true)
scaler: org.apache.spark.ml.feature.StandardScaler = stdScal_59a8f46478be

scala> val prepPipeline=new Pipeline().setStages(Array(assembler,scaler))
prepPipeline: org.apache.spark.ml.Pipeline = pipeline_884f7a401277

scala> val Array(trainRaw,testRaw)=df.randomSplit(Array(0.8,0.2),42)
trainRaw: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Latitude: double, Longitude: double ... 8 more fields]
testRaw: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Latitude: double, Longitude: double ... 8 more fields]

scala> println("Train rows: "+trainRaw.count()); println("Test rows: "+testRaw.count())
[Stage 8:>                                                          (0 + 8) [Stage 8:=======>                                                   (1 + 7) [Stage 8:===================================================>       (7 + 1)                                                                     Train rows: 336139
[Stage 11:>                                                         (0 + 8) [Stage 11:=============================>                            (4 + 4)                                                                             Test rows: 83744

scala> val pipelineModel=prepPipeline.fit(trainRaw)
[Stage 14:>                                                         (0 + 8) [Stage 14:==============>                                           (2 + 6)                                                                             pipelineModel: org.apache.spark.ml.PipelineModel = pipeline_884f7a401277

scala> val trainDF=pipelineModel.transform(trainRaw); val testDF=pipelineModel.transform(testRaw)
trainDF: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 10 more fields]
testDF: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 10 more fields]

scala> println("Feature vector size: "+trainDF.select("features").first().getAs[org.apache.spark.ml.linalg.Vector]("features").size)
Feature vector size: 5

scala> (2 to 6).foreach{k=>val model=new KMeans().setK(k).setSeed(42).setFeaturesCol("features").setPredictionCol("prediction").fit(trainDF); val preds=model.transform(testDF); val score=new ClusteringEvaluator().setFeaturesCol("features").setPredictionCol("prediction").setMetricName("silhouette").evaluate(preds); println("K="+k+" -> silhouette="+score)}
[Stage 18:>                                                         (0 + 8) [Stage 18:=======>                                                  (1 + 7) [Stage 18:=============================>                            (4 + 4)                                                                     [Stage 21:>                                                         (0 + 8) [Stage 21:=======>                                                  (1 + 7) [Stage 21:=====================>                                    (3 + 5) [Stage 21:=============================>                            (4 + 4)                                                                             26/05/09 20:23:07 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
[Stage 69:>                                                         (0 + 8) [Stage 69:=======>                                                  (1 + 7) [Stage 69:=====================>                                    (3 + 5) [Stage69:==================================================>       (7 + 1)                                                                             [Stage 72:>                                                         (0 + 8) [Stage 72:=======>                                                  (1 + 7) [Stage 72:====================================>                     (5 + 3)                                                                             [Stage 74:>                                               (0 + 8) [Stage 74:=======>                                                  (1 + 7)                                                                             K=2 -> silhouette=0.33041409986853026
[Stage 77:>                                                         (0 + 8) [Stage 77:=======>                                                  (1 + 7)                                                                             [Stage80:>                                                         (0 + 8) [Stage 80:=======>                                                  (1 + 7) [Stage 80:=====================>                                    (3 + 5)                                                              [Stage 128:>                                                        (0 + 8) [Stage 128:==============>                                          (2 + 6)                                                       [Stage 131:=======>                                                 (1 + 7) [Stage 131:=================================================>       (7 + 1)                                                [Stage 133:=======>                                                 (1 + 7) [Stage 133:=================================================>       (7 + 1)                                         K=3 -> silhouette=0.3122989573043786
[Stage 136:=======>                                                 (1 + 7) [Stage 136:===================================>                     (5 + 3)                                                                             [Stage139:>                                                        (0 + 8) [Stage 139:=======>                                                 (1 + 7) [Stage 139:============================>                            (4 + 4)                                                              [Stage 190:=======>                                                 (1 + 7)                                                                             [Stage 192:=======>                                               (1 + 7) [Stage 192:==============>                                          (2 + 6)                                                                             K=4 -> silhouette=0.32172990345900143
[Stage 195:=======>                                                 (1 + 7) [Stage 195:===================================>                     (5 + 3)                                                                             [Stage198:>                                                        (0 + 8) [Stage 198:=======>                                                 (1 + 7)                                                                             [Stage 246:=======>                                                 (1 + 7) [Stage 246:==============>                                          (2 + 6)                                                                             [Stage 249:============================>                            (4 + 4)                                                                             K=5 -> silhouette=0.34665664710701505
[Stage 254:=======>                                                 (1 + 7) [Stage 254:============================>                            (4 + 4)                                                                             [Stage257:>                                                        (0 + 8) [Stage 257:=======>                                                 (1 + 7) [Stage 257:=================================================>       (7 + 1)                                                              [Stage 305:=======>                                                 (1 + 7)                                                                             [Stage 310:============================>                            (4 + 4)                                                                             K=6 -> silhouette=0.32347392942795705

scala> val finalModel=new KMeans().setK(5).setSeed(42).setFeaturesCol("features").setPredictionCol("prediction").fit(trainDF)
[Stage 316:=======>                                                 (1 + 7) [Stage 316:=================================================>       (7 + 1)                                                                             finalModel: org.apache.spark.ml.clustering.KMeansModel = KMeansModel: uid=kmeans_9b7fdad3a44b, k=5, distanceMeasure=euclidean, numFeatures=5

scala> val finalPredictions=finalModel.transform(testDF)
finalPredictions: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 11 more fields]

scala> val evaluator=new ClusteringEvaluator().setFeaturesCol("features").setPredictionCol("prediction").setMetricName("silhouette")
evaluator: org.apache.spark.ml.evaluation.ClusteringEvaluator = ClusteringEvaluator: uid=cluEval_3e55c2664334, metricName=silhouette, distanceMeasure=squaredEuclidean

scala> val finalSilhouette=evaluator.evaluate(finalPredictions)
finalSilhouette: Double = 0.34665664710701505

scala> println("Final K=5 Silhouette score: "+finalSilhouette)
Final K=5 Silhouette score: 0.34665664710701505

scala> val baselineModel=new KMeans().setK(2).setSeed(42).setFeaturesCol("features").setPredictionCol("prediction").fit(trainDF)
[Stage 372:=================================================>       (7 + 1)                                                                             [Stage 375:>                                                        (0 + 8) [Stage375:=======>                                                 (1 + 7) [Stage 375:==============>                                          (2 + 6) [Stage 375:=================================================>       (7 + 1)                                                              baselineModel: org.apache.spark.ml.clustering.KMeansModel = KMeansModel: uid=kmeans_9ade4aed6683, k=2, distanceMeasure=euclidean, numFeatures=5

scala> val baselinePreds=baselineModel.transform(testDF)
baselinePreds: org.apache.spark.sql.DataFrame = [Latitude: double, Longitude: double ... 11 more fields]

scala> val baselineScore=new ClusteringEvaluator().setFeaturesCol("features").setPredictionCol("prediction").setMetricName("silhouette").evaluate(baselinePreds)
baselineScore: Double = 0.33041409986853026

scala> println("Baseline K=2 Silhouette score: "+baselineScore); println("Final K=5 Silhouette score: "+finalSilhouette)
Baseline K=2 Silhouette score: 0.33041409986853026
Final K=5 Silhouette score: 0.34665664710701505

scala> finalPredictions.groupBy("prediction").count().orderBy("prediction").show(false)
[Stage 431:=======>                                                 (1 + 7)                                                                             +----------+-----+
|prediction|count|
+----------+-----+
|0         |11708|
|1         |23235|
|2         |14644|
|3         |20069|
|4         |14088|
+----------+-----+


scala> finalPredictions.select("Primary Type","District","Time_Of_Day","Is_Night","Is_Weekend","prediction").show(20,false)
+-------------------+--------+-----------+--------+----------+----------+
|Primary Type       |District|Time_Of_Day|Is_Night|Is_Weekend|prediction|
+-------------------+--------+-----------+--------+----------+----------+
|motor vehicle theft|4       |Morning    |0       |0         |1         |
|criminal damage    |5       |Night      |1       |0         |1         |
|deceptive practice |4       |Morning    |0       |0         |1         |
|criminal damage    |4       |Night      |1       |0         |1         |
|motor vehicle theft|4       |Morning    |0       |0         |1         |
|criminal damage    |4       |Evening    |0       |0         |1         |
|battery            |4       |Night      |1       |1         |1         |
|assault            |4       |Afternoon  |0       |0         |1         |
|theft              |4       |Night      |1       |0         |1         |
|deceptive practice |4       |Morning    |0       |0         |1         |
|other offense      |4       |Morning    |0       |0         |1         |
|battery            |4       |Afternoon  |0       |0         |1         |
|deceptive practice |4       |Evening    |1       |0         |1         |
|assault            |5       |Afternoon  |0       |1         |1         |
|battery            |5       |Afternoon  |0       |0         |1         |
|narcotics          |5       |Morning    |0       |1         |0         |
|criminal damage    |4       |Night      |1       |0         |1         |
|theft              |4       |Afternoon  |0       |0         |1         |
|theft              |5       |Afternoon  |0       |1         |1         |
|theft              |5       |Morning    |0       |0         |1         |
+-------------------+--------+-----------+--------+----------+----------+
only showing top 20 rows


scala> finalModel.clusterCenters.zipWithIndex.foreach{case(c,i)=>println("Cluster "+i+": "+c)}
Cluster 0: [-0.027324868843108446,-0.1252959266893183,0.3043833417610703,0.035322947814623064,2.332530407324642]
Cluster 1: [-1.0580630950977932,0.8894901464458264,0.06883190830219538,0.11838009977705885,-0.3571827587098141]
Cluster 2: [0.5913470817185492,-0.6032777032845595,0.5898415760681096,0.9624049748613429,-0.41221781434686205]
Cluster 3: [0.5621345806664283,-0.2937103428988075,0.3598650543082785,-0.9493636527893583,-0.40031061009434754]
Cluster 4: [0.3748042316388755,-0.33583479453454956,-1.4645456384936013,0.1305077616113906,-0.35624255836608243]

(i-search)`':
