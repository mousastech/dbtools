# Using Hudi files in the Unity Catalog

A customer in Guatemala asked me: how to work with hudi tables in Unity Catalog? 
After googling it for the quickest solution, I got [this blog](https://www.onehouse.ai/blog/how-to-use-apache-hudi-with-databricks). Some versions and characteristics change over time and after adjusting some configurations I got solved. 

This example below was created with the following steps & configuration:

| Item | Configuration | 
| -------- | -------- | 
| Cluster Type    | Dedicated   | 
| Type    | Single Node   |
| Runtime    | 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)   |
| Unity Catalog    | On   |

### 1) Create a cluster or use an existing one with the following configuration:


<img src= "https://github.com/mousastech/dbtools/blob/c2168d64ece691b48ff62a8d6c43debd0510414e/hudi/img/0.Cluster.png?raw=true" size="50%">

**a) Cluster configuration: Advanced Option -> Spark, includes:**
<br>
<code>
spark.serializer org.apache.spark.serializer.KryoSerializer <br>
spark.kryo.registrator org.apache.spark.HoodieSparkKryoRegistrar <br>
spark.sql.sourcesUseV1SourceList hudi <br>
spark.sql.catalog.spark_catalog org.apache.spark.sql.hudi.catalog.HoodieCatalog <br>
spark.master local[*, 4] <br>
spark.sql.extensions org.apache.spark.sql.hudi.HoodieSparkSessionExtension <br>
spark.databricks.cluster.profile singleNode <br>
</code>

<img src= "https://github.com/mousastech/dbtools/blob/c2168d64ece691b48ff62a8d6c43debd0510414e/hudi/img/1.Spark%20configurations.png?raw=true" size="50%">

**b)Clusters Libraries: Install new - Maven:**

<code> org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 </code>

<img src= "https://github.com/mousastech/dbtools/blob/c2168d64ece691b48ff62a8d6c43debd0510414e/hudi/img/2.Library.png?raw=true" size="50%">

### 2)Run the instructions with the notebook provided

notebook <code> Read hudi tables.sql </code>

Hope you enjoy it.
 
