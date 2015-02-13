package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.types.ColumnType

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

import com.datastax.spark.connector.{ColumnRef, ColumnIndex}
import com.datastax.spark.connector.cql.{RegularColumn, PartitionKeyColumn, ColumnDef, TableDef}

class TupleColumnMapper[T <: Product : TypeTag : ClassTag] extends ColumnMapper[T] {

  override val classTag: ClassTag[T] = implicitly[ClassTag[T]]

  private def indexedColumnRefs(n: Int) =
    (0 until n).map(ColumnIndex)

  override def columnMap(tableDef: TableDef): ColumnMap = {

    val GetterRegex = "_([0-9]+)".r
    val cls = implicitly[ClassTag[T]].runtimeClass

    val constructor =
      indexedColumnRefs(cls.getConstructors()(0).getParameterTypes.length)

    val getters = {
      for (name @ GetterRegex(id) <- cls.getMethods.map(_.getName))
      yield (name, ColumnIndex(id.toInt - 1))
    }.toMap

    val setters =
      Map.empty[String, ColumnRef]

    SimpleColumnMap(constructor, getters, setters)
  }

  override def newTable(keyspaceName: String, tableName: String): TableDef = {
    val tpe = implicitly[TypeTag[T]].tpe
    val ctorSymbol = tpe.declaration(nme.CONSTRUCTOR).asMethod
    val ctorMethod = ctorSymbol.typeSignatureIn(tpe).asInstanceOf[MethodType]
    val ctorParamTypes = ctorMethod.params.map(_.typeSignature)
    require(ctorParamTypes.nonEmpty, "Expected a constructor with at least one parameter")

    val columnTypes = ctorParamTypes.map(ColumnType.fromScalaType)
    val columns = {
      for ((columnType, i) <- columnTypes.zipWithIndex) yield {
        val columnName = "_" + (i + 1).toString
        val columnRole = if (i == 0) PartitionKeyColumn else RegularColumn
        ColumnDef(columnName, columnRole, columnType)
      }
    }
    TableDef(keyspaceName, tableName, Seq(columns.head), Seq.empty, columns.tail)
  }
}
