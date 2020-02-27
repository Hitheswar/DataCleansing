package com.dama.dca.pipelines

import com.dama.dca.core.SparkApp
import com.dama.dca.cleansing.DataFrameCleansing._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

object DataCleansing extends App with SparkApp{
  import spark.implicits._

  var matches =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",")
    .load(System.getProperty("user.dir")+"/data/ipl_matches.csv").removeSpecialCharFromSchema().replaceColumnSpaceWith("_")

//  matches.show(2)
//  println(matches.columns)
//  matches.dropDuplicates()
//  matches.removeSpecialCharFromSchema().replaceColumnSpaceWith("_")
//  matches.na.drop()

//  matches.replaceAllNullColsWith("None").show()
//
//  matches.replaceColumnNullWithEmptyString("player_of_match").show()
//
//  matches.replaceColumnNullWithNone("player_of_match").show()
//
//  matches.replaceColumnNullWithZero("win_by_runs").show()

  matches.show()






}