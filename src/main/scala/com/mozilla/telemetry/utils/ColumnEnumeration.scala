/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
 package com.mozilla.telemetry.utils

import org.apache.spark.sql.Column

import scala.language.implicitConversions

/**
  * Base class for enumerations of Spark DataFrame column expressions.
  *
  * Enumerations are convenient here because they allow us to define a column
  * name as a val, and then get access to that Scala-level name as a string to
  * pass into Spark SQL expressions.
  *
  * You can also define a derived column by passing a SQL expression string into
  * the ColumnDefinition constructor; this logical definition is then available as `expr`.
  */
abstract class ColumnEnumeration extends Enumeration {

  protected case class ColumnDefinition(private val definition: Column = new Column(this.toString())) extends super.Val {
    /**
      * The name as given in the Scala code.
      *
      * For example, `val my_column = Val()` will have name "my_column".
      */
    def name: String = this.toString

    /**
      * The name of the column as a spark.sql.Column
      */
    def col: Column = new Column(name)

    /**
      * The logic defining the column as a spark.sql.Column
      */
    def expr: Column = definition.alias(name)
  }

  implicit def valueToColumnDefinition(value: Value): ColumnDefinition = value.asInstanceOf[ColumnDefinition]

  def names: List[String] = {
    values.toList.map(_.name)
  }

  def cols: List[Column] = {
    values.toList.map(_.col)
  }

  def exprs: List[Column] = {
    values.toList.map(_.expr)
  }

}
