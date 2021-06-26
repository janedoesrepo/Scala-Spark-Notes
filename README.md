# Scala Spark
Notes on the course [Big Data Analysis with Scala and Spark](https://www.coursera.org/learn/scala-spark-big-data) by Heather Mills


## Table of Contents
1. [Installation](https://github.com/janedoesrepo/Scala-Spark-Notes#installation)
2. [Resilient Distributed Datasets (RDDs)](https://github.com/janedoesrepo/Scala-Spark-Notes#resilient-distributed-datasets-rdds)
3. [Spark SQL](https://github.com/janedoesrepo/Scala-Spark-Notes#spark-sql)


## Installation

### Setup on Windows

#### Download and install
- Java: https://java.com/en/download/.
- Spark: https://spark.apache.org/downloads.html
- Hadoop: https://github.com/cdarlint/winutils

#### Set environment variables
- SPARK_HOME = \<path-to-spark-folder>, e.g. SPARK_HOME = "C:\spark-3.1.1-bin-hadoop2.7"
- HADOOP_HOME = \<path-to-hadoop-folder>, e.g. HADOOP HOME = "C:\hadoop\hadoop-2.7.7"
 
#### Add to PATH
- %SPARK_HOME%\bin
- %HADOOP_HOME%\bin


### Scala and Spark for Jupyter Notebook
ref: https://github.com/mariusvniekerk/spylon-kernel

#### Install spylon-kernel

```bash
pip install spylon-kernel
# or
conda install -c conda-forge spylon-kernel
```

#### Create a Scala Kernel

```bash
python -m spylon_kernel install
```

### Verify installation

Open a jupyter notebook, select the spylon-kernel and execute some code:

```scala
val x = 2
```

If you see something like the following output, everything works fine:

```bash
Intitializing Scala interpreter ...

Spark Web UI available at http://<your-computer>.com:4040
SparkContext available as 'sc' (version = 3.1.1, master = local[*], app id = local-1618231637628)
SparkSession available as 'spark'
```

## Resilient Distributed Datasets (RDDs)

### Basics

RDDs seem a lot like ***immutable*** sequential or parallel Scala collections. Most operations on RDDs, like Scala's immutable `List`, and Scala's parallel collections, are higher-order functions like map, flatMap, filter, reduce, fold, and aggregate.

```Scala
map[B](f: A => B): List[B] // Scala List
map[B](f: A => B): RDD[B]  // Spark RDD
```

```Scala
aggregate[B](z: => B)(seqop: (B, A) => B, combop: (B, B) => B): B // Scala
aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): B    // Spark RDD
```

Using RDDs in Spark feels a lot like normal Scala sequential/parallel collections, with added knowledge that your data is distributed across several machines.

#### Word Count: The "Hello, World!" of programming with large-scala data

```Scala
// Create an RDD
val rdd = spark.textFile("hdfs://...")

val count = rdd.flatMap(line => line.split(" ")) // separate lines into words
               .map(word => (word, 1))           // include something to count
               .reduceByKey(_ + _)               // sum up the 1s in the pairs
```

#### Creating RDDs

RDDs can be created in two ways:
 - Transforming an existing RDD.
 - From a `SparkContext`(or `SparkSession`) object with `parallelize` or `textFile`.

### Transformations and Actions

1. **Transformers** return new RDDs as results (they are `lazy`, the result is not immediately computed).
2. **Actions** compute a result based on an RDD, and either returns or saves the result (they are `eager`, the result is immediately computed).

#### Common Transformers

 - `map(f)`
```Scala
// Apply f to each element and return an RDD of the result
map[B](f: A => B): RDD[B] 
```

 - `flatMap(f)`
```Scala
// Apply f to each element and return an RDD of the contents of the iterators returned
flatMap[B](f: A => TraversableOnce[B]): RDD[B] 
```

 - `filter(pred)`
```Scala
// Apply predicate to each element and return an RDD of elements that have passed it
filter[B](pred: A => Boolen): RDD[A]
```

 - `distinct()`
```Scala
// Return RDD with duplicates removed
distinct(): RDD[B]
```

#### Common Actions

 - `collect()`
```Scala
// Return all elements from RDD
collect(): Array[T] 
```

 - `count()`
```Scala
// Return the number of elements in the RDD
count(): Long 
```

 - `take(num)`
```Scala
// Return the first num elements of the RDD
take(num: Int): Array[T]
```

 - `reduce(op)`
```Scala
// Combine the elements in the RDD together using `op function` and return results
reduce(op: (A, A) => A): A
```

 - `foreach(f)`
```Scala
// Apply function to each element in RDD.
foreach(f: T => Unit): Unit
```

#### Set-like Transformations

 - `union(other)`
```Scala
// Return an RDD containing elements from both RDDs
union(other: RDD[T]): RDD[T] 
```

 - `intersection(other)`
```Scala
// Return an RDD containing elements only found in both RDDs
intersection(other: RDD[T]): RDD[T] 
```

 - `subtract(other)`
```Scala
// Return an RDD with the contents of the other RDD removed
subtract(other: RDD[T]): RDD[T] 
```

 - `cartesian(other)`
```Scala
// Cartesian product with the other RDD
cartesian[U](other: RDD[U]): RDD[(T, U)] 
```

### Cache and Persistance: Logistic Regression Example

```Scala
val points = sc.textFile(...).map(parsePoints)
val w = Vector.zeros(d)
for (i <- 1 to numIterations) {
    val gradient = points.map { p =>
        (1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y * p.y
    }.reduce(_+_)
    w -= alpha * gradient
}
```

This code works fine. After reading data from disc and parsing the contents, we iterate `numIterations` over the logistic regression computation. However, transformations like `map` get re-evaluated every time an action is used. This means that `points` is being re-evaluated upon every iteration.

**Solution:**

To tell Spark to cache an RDD in memory, simply call `persist()` or `cache` on it.

```Scala
val points = sc.textFile(...).map(parsePoints).persist()
```

### Reduce Operations on RDDs

 - `fold(zeroval)(combop)`
```Scala
fold(z: A)(f: (A, A) => A): A 
```
 - `aggregate(zeroval)(seqop, combop)`
```Scala
aggregate(z: => B)(seqop: (B, A) => B, combop: (B, B) => B): B 
```

Spark doesn't even give you the option to use foldLeft/foldRight, which means that if you have to change the return type of your reduction operation, your only choice is to use aggregate.


## Spark SQL
- allows seamless intermixing of SQL queries with Scala
- adapts optimizations used in databases to Spark jobs

**Goals:**
- Support relational processing both within Spark programs (on RDDs) and on external data sources with a friendly API
- High performance, achieved by using techniques from research in databases
- Easily support new data sources such as semi-structured data and external databases

**Main APIs:**
- SQL literal syntax
- DataFrames
- Datasets

### DataFrames
- DataFrames are, conceptually, RDDs full of records with a known schema.
- DataFrames are untyped (unlike RDD[T]).
- Transformations on DataFrames are untyped transformations.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession
    .builder()
    .appName("My App")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()

```

#### Creating DataFrames
1. From an existing RDD
    1. with schema inference
    2. with an explicit schema
2. Reading in a specific data source from file

**(1.A)** Create <code>DataFrame</code> from <code>RDD</code>, schema reflectively inferred
Given a pair RDD, RDD[(T1, T2, ..., TN)], a DataFrame can be created with its schema automatically inferred by simply using the toDF method.

```scala
val tupleRDD = ... // Assume RDD[(Int, String, String, String)]
val tupleDF = tupleRDD.toDF("id", "name", "city", "country") // column names
```

If you already have an RDD containing some kind of case class isntance, then Spark can infer the attributes from the case class's fields.

```scala
case class Person(id: Int, name: String, city: String)
val peopleRDD = ... // Assume RDD[Person]
val peopleDF = peopleRDD.toDF
```

**(1.B)** Create <code>DataFrame</code> from existing <code>RDD</code>, schema explicitly specified
Sometimes it's not possible to create a DataFrame with a pre-determined case class as its schema. For these cases, it's possible to explicitly specify a schema.
It takes three steps:
1. Create an RDD of <code>Rows</code> from the original RDD.
2. Create the schema represented by a <code>StructType</code> matching the structure of <code>Rows</code> in the RDD created in step 1.
3. Apply the schema to the RDD of <code>Rows</code> via <code>createDataFrame</code> method prrovided by <code>SparkSession</code>

```scala
// Given
case class Person(name: String, age: Int)
val peopleRDD = sc.textFile(...) // Assume RDD[Person]

// The schema is encoded in a string
val schemaString = "name age" 

// Generate the schema based on the schemaString
val fields = schemaString.split(" ")
  .map(fieldName => StructField(fieldName, StringType, nullable = true))
val schema = StructType(fields)
       
// Convert records of the RDD (people) to Rows
val rowRDD = peopleRDD
  .map(_.split(","))
  .map(attributes => Row(attributes(0), attributes(1).trim))

// Apply the schema to the RDD
val peopleDF = spark.createDataFrame(rowRDD, schema)
                                         
```

**(2)** Create <code>DataFrame</code> by reading in a data source from file
Using the <code>SparkSession</code> object, you can read in semi-structured/structured data by using the <code>read</code> method. For example, to read in data and infer a schema from a JSON file:

```scala
// 'spark' is the SparkSession object we created earlier
val df = spark.read.json("<some path>.json")
```

#### Using SQL Literals

Once you have a <code>DataFrame</code> to operate on, you can now freely write familiar SQL syntax to operate on your dataset!
Given a <code>DataFrame</code> called <code>peopleDF</code>, we just have to register our <code>DataFrame</code> as a temporary SQL view first:

```scala
// Register the DataFrame as a SQL temporary view
peopleDF.createOrReplaceTempView("people")
// This essentially gives a name to our DataFrame in SQL
// so we can refer to it in a SQL FROM statement

// SQL literals can be passed to Spark SQL's sql method
val adultsDF = spark.sql("SELECT * FROM people WHERE age > 17")
```

The SQL statements avaiable are largely what's available in HiveQL. This includes standard SQL statements.

**Supported Spark SQL syntax:**<br/>
https://docs.datastax.com/en/archived/datastax_enterprise/4.6/datastax_enterprise/spark/sparkSqlSupportedSyntax.html

**For a HiveQL cheatsheet:**<br/>
http://hortonworks.com/wp-content/uploads/2016/05/Hortonworks.CheatSheet.SQLtoHive.pdf

**For an updated list of suported Hive features in Spark SQL, the official Spark SQL docs enumerate:**<br/>
https://spark.apache.org/docs/latest/sql-programming-guide.html#supported-hive-features

**Example:**

```scala
case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)

// DataFrame with schema defined in Employee case class
val employeeDF = sc.parallelize(...).toDF

// Register temp view
employeeDF.createOrReplaceTempView("employees")

val sydneyEmployeesDF = spark
  .sql("""SELECT id, lname
          FROM employees
          WHERE city = "Sydney"
          ORDER BY id""")
```

### DataFrames Deep Dive

 - available DataFrame data types
 - basic operations on DataFrames
 - ...

```scala
// Accessing sql datatypes requires import
import org.apache.spark.sql.types._
```

#### Selecting and working with columns

You can select and work with columns in three ways:

1. Using $-notation
```scala
df.filter($"age" > 17)
```

2. Referring to the DataFrame
```scala
df.filter(df("age") > 17)
```

3. Using SQL query string
```scala
df.filter("age > 17")
```


**Example:**

```scala
case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)
val employeeDF = sc.parallelize(...).toDF

val sydneyEmployeesDF = employeeDF
  .select("id", "lname")
  .where("city == 'Sydney'")
  .orderBy("id")
```

```scala
// Accessing aggregation functions on groupBy
import org.apache.spark.sql.functions._
```

#### Cleaning DataFrames

**Dropping records with unwanted values:**
- ```drop``` drops rows that contail null or NaN values in **any** column
- ```drop("all")``` drops rows that contain null or NaN values in **all** columns
- ```drop(Array("id", "name"))``` drops rows that contail null or NaN values in the **specified** columns

**Replacing unwanted values:**
- ```fill(0)``` replaces all occurrences of null or NaN in **numeric columns** with a **specified value**
- ```fill(Map("minBalance" -> 0))``` replaces all occurrences of null or NaN in **specified columns** with the **specified values**
- ```replace(Array("id"), Map(1234 -> 8923))``` replaces a **specified value** (1234) in a **specified column** (id) with a **specified replalcement value** (8923)

#### Joins on DataFrames

**Performing joins:**
Given two DataFrames, df1 and df2 each with a column called id, we can perform an inner join as follows:
```scala
df1.join(df2, $"df1.id" === $"df2.id")
```

It's possible to change the join type by passing an additional string parameter to join specifying wwhich type of join to perform. E.g.,
```scala
df1.join(df2, $"df1.id" === $"df2.id", "right_outer")
```

#### Optimizations on DataFrames

When using DataFrames, Spark takes care of optimization for us. This is mainly done by two components:
 - **Catalyst** query optimizer
 - **Tungsten** off-heap serializer
 
Compared to RDDs, DataFrames are (like Databases) very structured and yields lots of opportunities for optimization.

Assuming **Catalyst** has:
 - has full knowledge and understanding of all data types
 - knows the exact schema of our data
 - has detailed knowledge of the computations wew'd like to do
 
This makes it possible for us to do optimizations like:
 - Reordering operations
 - Reduce the amount of data we must read
 - Pruning unneeded partitioning
 
Since our data types are restricted to Spark SQL data types, **Tungsten** can provide:
 - highly-specialized data encoders
 - column-based format
 - off-heap (free from garbage collection)
 
Taken together, Catalyst and Tungsten offer ways to significantly speed up your code, even if you write it inefficiently initially.

#### Limitations

**DataFrames are untyped**. Your code compiles, but you get runtime exceptions when you attempt to run a query on a column that doesn't exist. It would be nice if this was caught at compile time like we're used to in Scala.

**Limited Data Types**. If your data can't be expressed by <code>case classes</code> and standard Spark SQL data types, it may be difficult to ensure that a Tungsten encoder exists for your data type.

**Requires Semi-Structured/Structured Data**. If your unstructured data cannot be reformulated to adhere to some kind of schema, it would be better to use RDDs.

### Datasets

```scala
val averagePrices = averagePricesDF.collect()
// averagePrices: Array[org.apache.spark.sq.Row]

averagePrices.head.schema.printTreeString()
// root
//  |-- zip: integer (nullable = true)
//  |-- avg(price): double (nullable = true)

val averagePricesAgain = averagesPrices.map {
    row => (row(0).asInstanceOf[Int], ro(1).asInstanceOf[Double])
}
```

DataFrames are acually Datasets:

```scala
type DataFrame = DataSet[Row]
```

**What is a Dataset?**
- Datasets can be thought of as **typed** distributed colelctions of data.
- The Dataset API unifies the DataFrame and RDD APIs (mix and match).
- Datasets require structured/semi-structured data. Schemas and Encoders are a core part of Datasets.

Think of Datasets as a compromise between RDDs & DataFrames. You get more type information on Datasets than on DataFrames, and you get more optimizations on Datasets than you get on RDDs.

```scala
listingsDS.groupByKey(l => l.zip)      // looks like groupByKey on RDDs!
        .agg(avg($"price".as[Double])  // looks like our DataFrame operators!
```

Datasets can be used when you want a mix of functional

#### Creating DataSets

Just use the <code>.toDS</code> convenience method on DataFrames, RDDs or common Scala types:
```scala
import spark.implicits._

myDF.toDS
myRDD.toDS
List("yay", "ohnoes", "hooray!").toDS
```

On Datasets, typed operations tend to act on TypedColumns. To create a TypedColumn, all you have to do is to call <code>.as[...]</code> on your (untyped) Column:

```scala
$"price".as[Double] // this now represents a typed Column
```

#### Transformations on Datasets

The Dataset API includes both untyped and typed transformations:
 - **untyped transformations** the transformations we learned on DataFrames
 - **typed transformations** typed variants of many DataFrame transformations plus additional transformatins such as RDD-like higher-order functions.
 
Datasets are missing an important transformation that we often used on RDDs: **reduceByKey**.

**Challenge:** Emulate the semantics of reduceByKey on a Dataset using Dataset operations presented so far. Assume the following data set:


```scala
val keyValues = List((3,"Me"), (1,"Thi"), (2,"Se"), (3,"ssa"), (1,"sIsA"), (3,"ge:"), (3,"-)"), (2,"cre"), (2,"t"))

import spark.implicits._
val keyValuesDS = keyValues.toDS

keyValuesDS.groupByKey(p => p._1)
           .mapGroups((k, vs) => (k, vs.foldLeft("")((acc, p) => acc + p._2)))
           .sort($"_1").show()
```

    +---+----------+
    | _1|        _2|
    +---+----------+
    |  1|   ThisIsA|
    |  2|    Secret|
    |  3|Message:-)|
    +---+----------+
    
    




    keyValues: List[(Int, String)] = List((3,Me), (1,Thi), (2,Se), (3,ssa), (1,sIsA), (3,ge:), (3,-)), (2,cre), (2,t))
    import spark.implicits._
    keyValuesDS: org.apache.spark.sql.Dataset[(Int, String)] = [_1: int, _2: string]




**The only issue with this approach is this disclaimer in the API docs for mapGroups:**
This function does not support partial aggregation, and as a result requires shuffling all the data in the Dataset. If an application intends to perform an aggregation over each key *(which is exactly what we're doing)*, it is best to use the reduce function or an org.apache.spark.sql.expressions#Aggregator.


```scala
val keyValues = List((3,"Me"), (1,"Thi"), (2,"Se"), (3,"ssa"), (1,"sIsA"), (3,"ge:"), (3,"-)"), (2,"cre"), (2,"t"))

import spark.implicits._
val keyValuesDS = keyValues.toDS

keyValuesDS.groupByKey(p => p._1)
           .mapValues(p => p._2)
           .reduceGroups((acc, str) => acc + str)  
           .sort($"key").show()
```

    +---+----------------------------------+
    |key|ReduceAggregator(java.lang.String)|
    +---+----------------------------------+
    |  1|                           ThisIsA|
    |  2|                            Secret|
    |  3|                        Message:-)|
    +---+----------------------------------+
    
    




    keyValues: List[(Int, String)] = List((3,Me), (1,Thi), (2,Se), (3,ssa), (1,sIsA), (3,ge:), (3,-)), (2,cre), (2,t))
    import spark.implicits._
    keyValuesDS: org.apache.spark.sql.Dataset[(Int, String)] = [_1: int, _2: string]




**That works, but the docs also suggested an Aggregator!**

#### Aggregators


A class that helps you generically aggregate data. Kind of like the aggregate method we saw on RDDs.

```scala
class Aggregator[-IN, BUF, OUT]
```
- **IN** is  the input type to the aggregator. When using an aggregator after groupByKey, this is the type that represents the value in the key/value pair.
- **BUF** is  the intermediate type during aggregation.
- **OUT** is the type of the ouput of the aggregation.

**Emulating reduceByKey with an Aggregator:**


```scala
val keyValues = List((3,"Me"), (1,"Thi"), (2,"Se"), (3,"ssa"), (1,"sIsA"), (3,"ge:"), (3,"-)"), (2,"cre"), (2,"t"))

import spark.implicits._
val keyValuesDS = keyValues.toDS

import org.apache.spark.sql.expressions.Aggregator

val strConcat = new Aggregator[(Int, String), String, String] {
    def zero: String = ""
    def reduce(b: String, a: (Int, String)): String = b + a._2
    def merge(b1: String, b2: String): String = b1 + b2
    def finish(r: String): String = r
}.toColumn

keyValuesDS.groupByKey(pair => pair._1)
           .agg(strConcat.as[String])
```


    <console>:49: error: object creation impossible, since:

    it has 2 unimplemented members.

    /** As seen from <$anon: org.apache.spark.sql.expressions.Aggregator[(Int, String),String,String]>, the missing signatures are as follows.

     *  For convenience, these are usable as stub implementations.

     */

      def bufferEncoder: org.apache.spark.sql.Encoder[String] = ???

      def outputEncoder: org.apache.spark.sql.Encoder[String] = ???

    

           val strConcat = new Aggregator[(Int, String), String, String] {

                               ^

    


**We are missing two method implementations. What's an Encoder?**

#### Encoders

Encoders are what convert your data between JVM objects and Spark SQL's specialized internal (tabular) representation. **They're required by all Datasets!**

Encoders are highly specialized, optimized code generators that generate custom bytecode for serialization and deserialization of your data. The serialized data is stored using Spark internal Tungsten binary format, alllowing for operations on serialized data and improved memory utilization.

**What sets them apart from regular Java or Kryo serialization:**
 - Limited to and optimal for primitives and case classes, Spark SQL data types, which are well-understood.
 - **They contain schema information**, which makes these highly optimized code generators possible, and enables optimization based on the shape pf the data. Since Spark understands the structure of data in Datasets, it can create a more optimal layout in memory when caching Datasets.
 - Uses significantly less memory than Kryo/Java serialization
 - \>10x faster than Kryo serialization (Java serialization order of magnitude slower)
 
**Two ways to introduce encoders:**
 - **Automatically** (generally the case) via implicits form a SparkSession.
 - **Explicitly** via <code>org.apache.spark.sql.Encoder</code>, which contains a large selection of methods for createing Encoders from Scala primitive types and Products.
 
**Example:**
```scala
Encoders.scalaInt //Encoder[Int]
Encoders.STRING // Encoder[String]
Encoders.product[Person] // Encoder[Person], where Person extends Product/is a case class
```

**Emulating reduceByKey with an Aggregator (ctd.):**


```scala
val keyValues = List((3,"Me"), (1,"Thi"), (2,"Se"), (3,"ssa"), (1,"sIsA"), (3,"ge:"), (3,"-)"), (2,"cre"), (2,"t"))

import spark.implicits._
val keyValuesDS = keyValues.toDS

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

val strConcat = new Aggregator[(Int, String), String, String] {
    def zero: String = ""
    def reduce(b: String, a: (Int, String)): String = b + a._2
    def merge(b1: String, b2: String): String = b1 + b2
    def finish(r: String): String = r
    override def bufferEncoder: Encoder[String] = Encoders.STRING
    override def outputEncoder: Encoder[String] = Encoders.STRING
}.toColumn

keyValuesDS.groupByKey(pair => pair._1)
           .agg(strConcat.as[String])
           .sort($"key").show()
```

    +---+---------------------+
    |key|$anon$1(scala.Tuple2)|
    +---+---------------------+
    |  1|              ThisIsA|
    |  2|               Secret|
    |  3|           Message:-)|
    +---+---------------------+
    
    




    keyValues: List[(Int, String)] = List((3,Me), (1,Thi), (2,Se), (3,ssa), (1,sIsA), (3,ge:), (3,-)), (2,cre), (2,t))
    import spark.implicits._
    keyValuesDS: org.apache.spark.sql.Dataset[(Int, String)] = [_1: int, _2: string]
    import org.apache.spark.sql.expressions.Aggregator
    import org.apache.spark.sql.{Encoder, Encoders}
    strConcat: org.apache.spark.sql.TypedColumn[(Int, String),String] = $anon$1(staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS `value`, input[0, string, true].toString, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false))




#### When to use Datasets over DataFrames over RDDs?

**Use Datasets when...**
 - you have structured/semi-structured data
 - you want typesafety
 - you need to work with functional APIs
 - you need good performance, but it doesn't have to be the best
 
**Use DataFrames when...**
 - you have structured/semi-structured data
 - you want the best possible performance, automatically optimzied for you
 
**Use RDDs when...**
 - you have unstructured data
 - you need to fine-tune and manage low-level details of RDD computations
 - you have complex data types that cannot be serialized with <code>Encoders</code>

#### Limitations

1. **Catalyst can't optimize all operations**

    While **relational filter operations**, e.g. ```ds.filter($"city".as[String] === "Boston")```, can be optimized, Catalyst 
    cannot optimize **functional filter operations** like ```ds.filter(p => p.city == "Boston")```.


2. **Takeaways:**
 - When using Datasets with high-order functions like map, you miss out on many Catalyst optimizations.
 - When using Datasets with relationqal operations like select, you can get all of Catalyst's optimizations.
 - Though not all operations on Datasets benefit from Catalyst's optimizations, Tungsten is still always running under the hood of Datasets, storing and organizing data in a highly optimized way, which can result in large speedups over RDDs.


3. **Limited Data Types**

4. **Requires semi-structured/structured data**
