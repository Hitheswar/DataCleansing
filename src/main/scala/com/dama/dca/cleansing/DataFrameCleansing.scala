package com.dama.dca.cleansing

import org.apache.spark.annotation.{DeveloperApi, InterfaceStability}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object DataFrameCleansing {
  implicit class SchemaValidation(dataFrame: DataFrame) {
    def replaceColumnSpaceWith(str:String):DataFrame = {
      var df:DataFrame = dataFrame
      for(col <- dataFrame.columns){
        df = df.withColumnRenamed(col,col.replaceAll("\\s", str))
      }
      df
    }
    def removeSpecialCharFromSchema():DataFrame ={
      var df:DataFrame = dataFrame
      for(col <- dataFrame.columns){
        df = df.withColumnRenamed(col,col.replaceAll("""[+._(),]+""", ""))
      }
      df
    }
  }
  implicit class CleansingNull(dataFrame: DataFrame){
    def removeNullValues(): DataFrame = {
      dataFrame.na.drop()
    }
    def replaceAllNullColsWith(value:String):DataFrame = {
      dataFrame.na.fill(value)
    }
    def replaceColumnNullWithNone(ColName:String):DataFrame = {
      dataFrame.na.fill("None",Array(ColName))
    }
    def replaceColumnNullWithZero(ColName:String):DataFrame = {
      dataFrame.na.fill(0,Array(ColName))
    }
    def replaceColumnNullWithEmptyString(ColName:String):DataFrame = {
      dataFrame.na.fill(" ",Array(ColName))
    }
  }

  implicit class dataManipulation(dataFrame: DataFrame) {
    def addSequenceIdWithColName(ColumnName: String): DataFrame = {

      null
    }

  }
}

