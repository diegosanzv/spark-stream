package com.ripjar.spark.data

import com.github.nscala_time.time.Imports.DateTime

trait DataList extends Serializable

class StringList(val value: List[String]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

class BooleanList(val value: List[Boolean]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

class LongList(val value: List[Long]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

class DoubleList(val value: List[Double]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

class DateTimeList(val value: List[DateTime]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

class DataItemList(val value: List[DataItem]) extends DataList { override def toString(): String = value.mkString("[", ",", "]") }

