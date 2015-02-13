package com.datastax.spark.connector.types

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{UUID, Date}

import scala.reflect.runtime.universe._
import org.scalatest.{WordSpec, GivenWhenThen, Matchers}

class ColumnTypeSpec extends WordSpec with Matchers with GivenWhenThen {

  "A ColumnType companion object" should {

    "throw InvalidArgumentException if given unsupported type" in {
      an [IllegalArgumentException] should be thrownBy ColumnType.fromScalaType(typeOf[ColumnTypeSpec])
    }

    "allow to obtain a proper ColumnType" when {

      "given a Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[Boolean]) === BooleanType)
      }
      "given a java.lang.Boolean should return BooleanType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Boolean]) === BooleanType)
      }
      "given an Int should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Int]) === IntType)
      }
      "given an java.lang.Integer should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Integer]) === IntType)
      }
      "given a Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[Long]) === BigIntType)
      }
      "given a java.lang.Long should return BigIntType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Long]) === BigIntType)
      }
      "given a Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[Float]) === FloatType)
      }
      "given a java.lang.Float should return FloatType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Float]) === FloatType)
      }
      "given a Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[Double]) === DoubleType)
      }
      "given a java.lang.Double should return DoubleType" in {
        assert (ColumnType.fromScalaType(typeOf[java.lang.Double]) === DoubleType)
      }
      "given a String should return VarcharType" in {
        assert (ColumnType.fromScalaType(typeOf[String]) === VarCharType)
      }
      "given a java.util.Date should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.util.Date]) === TimestampType)
      }
      "given a java.sql.Date should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[java.sql.Date]) === TimestampType)
      }
      "given a org.joda.time.DateTime should return TimestampType" in {
        assert (ColumnType.fromScalaType(typeOf[org.joda.time.DateTime]) === TimestampType)
      }
      "given a ByteBuffer should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[ByteBuffer]) === BlobType)
      }
      "given an Array[Byte] should return BlobType" in {
        assert (ColumnType.fromScalaType(typeOf[Array[Byte]]) === BlobType)
      }
      "given an UUID should return UUIDType" in {
        assert (ColumnType.fromScalaType(typeOf[UUID]) === UUIDType)
      }
      "given a List[String] should return ListType(VarcharType)" in {
        assert (ColumnType.fromScalaType(typeOf[List[String]]) === ListType(VarCharType))
      }
      "given a Set[InetAddress] should return SetType(InetType)" in {
        assert (ColumnType.fromScalaType(typeOf[Set[InetAddress]]) === SetType(InetType))
      }
      "given a Map[Int, Date] should return MapType(IntType, TimestampType)" in {
        assert (ColumnType.fromScalaType(typeOf[Map[Int, Date]]) === MapType(IntType, TimestampType))
      }
      "given an Option[Int] should return IntType" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Int]]) === IntType)
      }
      "given an Option[Vector[Int]] should return ListType(IntType)" in {
        assert (ColumnType.fromScalaType(typeOf[Option[Vector[Int]]]) === ListType(IntType))
      }
    }
  }

}
