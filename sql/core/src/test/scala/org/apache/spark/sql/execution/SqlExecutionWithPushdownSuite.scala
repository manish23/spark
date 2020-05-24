/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.optimizer.{ColumnPruning, PushDownPredicate, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.optimizer.{ColumnPruningBackport30, ColumnPruningJ1Rule, PushDownPredicatesBackport30}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceStrategyBackport30, PruneFileSourcePartitions, SchemaPruningBackport30}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetSchemaPruning, ParquetSchemaPruningSuite}
import org.apache.spark.sql.internal.SQLConf.NESTED_SCHEMA_PRUNING_ENABLED
import org.apache.spark.sql.types.StructType
import org.scalactic.Equality
//import org.apache.spark.sql.execution.datasources.FileSourceStrategyV2

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class SqlExecutionWithPushdownSuite extends SparkFunSuite {

  test("test") {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("test")
      .config(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key, "true")
      .config(SQLConf.PARQUET_RECORD_FILTER_ENABLED.key, "true")
      .config(SQLConf.PARQUET_BINARY_AS_STRING.key, "true")
        // Disable adding filters from constraints because it adds, for instance,
        // is-not-null to pushed filters, which makes it hard to test if the pushed
        // filter is expected or not (this had to be fixed with SPARK-13495).
      .config(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
      .config(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key, "true")

      .config("spark.sql.optimizer.nestedPredicatePushdown.enabled", "true")
//      .config(SQLConf.NESTED_PREDICATE_PUSHDOWN_ENABLED.key, "true")

      .config("spark.sql.optimizer.expression.nestedPruning.enabled", "true")
//      .config(SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key, "true")

      .config("spark.sql.optimizer.serializer.nestedSchemaPruning.enabled", "true")
//      .config(SQLConf.SERIALIZER_NESTED_SCHEMA_PRUNING_ENABLED.key, "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.tungsten.enabled", "false")

      .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
        Seq(
          PushPredicateThroughJoin.ruleName,
          PushDownPredicate.ruleName,
          ParquetSchemaPruning.ruleName//,
//          ColumnPruning.ruleName
        ).mkString(","))

      .getOrCreate()
//    spark.experimental.extraOptimizations = Seq(PushDownPredicatesJ1, ParquetSchemaPruning)

    spark.extensions.injectOptimizerRule(ColumnPruningJ1Rule)
//    spark.extensions.injectPlannerStrategy(DataSourceStrategyJ1Strategy)

    // Shorthand for running a query using our SQLContext
    val sql = spark.sql _

    spark.experimental.extraOptimizations = Seq(ColumnPruningBackport30, SchemaPruningBackport30, PushDownPredicatesBackport30)
//    spark.experimental.extraOptimizations = Seq(ColumnPruning, ParquetSchemaPruning, PushDownPredicate)
//    spark.experimental.extraStrategies = new DataSourceStrategyJ1(spark.sqlContext.conf) :: Nil

    import org.apache.spark.sql.functions._
    import spark.implicits._

    FileUtils.deleteQuietly(new File("/tmp/parquet"))

    if(! new File("/tmp/parquet").exists())
    {
      val clients = Stream.range(0, 10).map(it => new Client(it + "", "ucn_" + it, "p_col1_" + it, "p_col2_" + it))
      val balances = Stream.range(0, 10).map(it => new Balance(it + "",
        "p_col1_" + it, "p_col2_" + it, it, clients))
      val positions = Stream.range(0, 10).map(it => new Position(it + "",
        "p_col1_" + it, "p_col2_" + it, "model_" + it, clients(2), balances)).toSeq
      spark.sparkContext.parallelize(positions).toDF().write.mode(SaveMode.Overwrite).parquet("/tmp/parquet")
      spark.read.parquet("/tmp/parquet").printSchema()
    }

//    // balances.list.element.id
//    val parquetSchema = """
//      |Parquet form:
//      |message spark_schema {
//      |  optional binary pos_model (STRING);
//      |  optional group balances (LIST) {
//      |    repeated group list {
//      |      optional group element {
//      |        optional binary id (STRING);
//      |        required double amount;
//      |        optional group bal_clients (LIST) {
//      |          repeated group list {
//      |            optional group element {
//      |              optional binary id (STRING);
//      |              optional binary ucn (STRING);
//      |            }
//      |          }
//      |        }
//      |      }
//      |    }
//      |  }
//      |}
//    """.stripMargin

    //    val df2 = spark.read.parquet("/tmp/parquet")
    //      .select($"pos_model", $"pos_client.ucn", explode('balances).as("balances"))
    ////      .select($"pos_model", $"pos_client_ucn", $"balances", explode($"balances.bal_clients")as("bal_clients"))
    ////      .select($"pos_model", $"pos_client_ucn", $"balances.amount"/*, $"bal_clients.ucn"*/)
    //      .filter($"pos_model" === "model_1"
    ////        and $"bal_clients.ucn" === "5"
    //        and $"balances.amount" >= 0
    //        and $"pos_client.ucn" === "")
    //
    //    df2.explain(true)

    //      val df1 = spark.sparkContext.parallelize(positions).toDF()
    //        .select('id, explode('balances).as("balances"))
    //        .select('id, 'balances, explode($"balances.clients")as("clients"))
    //        .filter($"clients.ucn" === "5" and $"balances.id" >= 0)
    //
    //      df1.explain(true)

    //      val df = positions.toDF()
    //        .select('id, $"balances.id")
    //        .filter($"balances.amount" >= 6)
    //
    //      df.explain(true)

//    val df3 = spark.read.parquet("/tmp/parquet")
//      .select($"pos_model", $"balances")

//    spark.read.parquet("/tmp/parquet").createOrReplaceTempView("Loans")
//    var expectedSchema =
//      """
//        |StructType(
//        | StructField(pid,StringType,true),
//        | StructField(p_col1,StringType,true),
//        | StructField(p_col2,StringType,true),
//        | StructField(pos_model,StringType,true),
//        | StructField(pos_client,
//        |   StructType(StructField(ucn,StringType,true)),true),
//        |   StructField(balances,ArrayType(
//        |       StructType(StructField(bid,StringType,true),
//        |       StructField(amount,DoubleType,true)),true),true))
//      """.stripMargin
//    expectedSchema = StringUtils.replaceAll(expectedSchema, "\n", "")
////    expectedSchema = StringUtils.replaceAll(expectedSchema, " ", "")
//    expectedSchema = "StructType(StructField(pid,StringType,true), StructField(p_col1,StringType,true), StructField(p_col2,StringType,true), StructField(pos_model,StringType,true), StructField(pos_client,StructType(StructField(ucn,StringType,true)),true), StructField(balances,ArrayType(StructType(StructField(bid,StringType,true), StructField(amount,DoubleType,true)),true),true))"
////      """
////        |StructType(StructField(pid,StringType,true), StructField(p_col1,StringType,true), StructField(p_col2,StringType,true), StructField(pos_model,StringType,true), StructField(pos_client,StructType(StructField(ucn,StringType,true)),true), StructField(balances,ArrayType(StructType(StructField(bid,StringType,true), StructField(amount,DoubleType,true)),true),true))
////      """.stripMargin
////    expectedSchema = "struct<employer:struct<company:struct<name:string,address:string>>>"
//
//    val query = sql("select pos_model, pos_client.ucn as pos_client_ucn, explode(balances) as balances from Loans")
//      .selectExpr("pos_model", "pos_client_ucn", "balances.bid as balances_bid", "balances.amount as balances_amt")
//      .where($"pos_model" === "model_1" and $"pos_client_ucn" === "ucn_2" and $"balances_bid" === "9")
//    checkScan(query, expectedSchema)
//
//    println("")

    val df3 = spark.read.parquet("/tmp/parquet")
      .select($"pos_model", $"pos_client.ucn".as("pos_client_ucn"), explode($"balances").as("balances"))
      .select($"pos_model", $"pos_client_ucn", $"balances.bid".as("balances_bid"), $"balances.amount".as("balances_amt"))
      .filter($"pos_model" === "model_1" and $"pos_client_ucn" === "ucn_2" and $"balances_bid" === "9")
//      .filter($"pos_model" === "model_1" /*and $"pos_client_ucn" === "2"*/ and $"balances.list.element.id" === "1")

//    val df3 = spark.read.parquet("/tmp/parquet")
//      .select($"pos_model", $"pos_client.ucn".as("pos_client_ucn"), explode($"balances.id").as("balances_id"))
//      .select($"pos_model", $"pos_client_ucn", $"balances_id")
//      .filter($"pos_model" === "model_1" and $"pos_client_ucn" === "2" and $"balances_id" === "9")

//    val df3 = spark.read.parquet("/tmp/parquet")
//      .select(explode($"balances.id").as("balances_id"))
//      .filter($"balances_id" === "9")

    val rows = df3.collect()

    df3.explain(true)

//    val df4 = spark.read.parquet("/tmp/parquet")
//      .select($"pos_model", explode($"balances").as("balances"))
//      .select($"pos_model", $"balances.id")
//      .filter($"pos_model" === "model_1")
//      .collectAsList()
//
//    println(rows)
////    ExpressionEncoder().toRow(null)
//
//    //    val df4 = spark.read.parquet("/tmp/parquet")
//    //      .select($"pos_model", explode($"balances").as("balances"))
//    //      .select($"pos_model"/*, $"balances.id".as("bid")*/)
//    //      .filter($"pos_model" === "5" /*$"balances.id" > 0*/)
//    //
//    //    df4.collect()
//    //    df4.explain(true)

    assert(true)
    println("Manish : test completed")

  }

//  val encoder = ExpressionEncoder()

  private val schemaEquality = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => a.sameType(otherType)
        case _ => false
      }
  }

  protected def checkScan(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchemata(df, expectedSchemaCatalogStrings: _*)
    // We check here that we can execute the query without throwing an exception. The results
    // themselves are irrelevant, and should be checked elsewhere as needed
    df.collect()
  }

  private def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      df.queryExecution.executedPlan.collect {
        case scan: FileSourceScanExec => scan.requiredSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }

}


case class Position(pid: String, p_col1: String, p_col2: String,
                    pos_model: String, pos_client: Client, balances: Seq[Balance])
case class Balance(bid: String, b_col1: String, b_col2: String,
                   amount: Double, bal_clients: Seq[Client])
case class Client(cid: String, ucn: String, c_col1: String, c_col2: String)


