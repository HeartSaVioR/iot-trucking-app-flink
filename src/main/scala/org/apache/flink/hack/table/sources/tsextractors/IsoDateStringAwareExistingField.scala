package org.apache.flink.hack.table.sources.tsextractors

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.expressions.{Cast, Expression, ResolvedFieldReference}
import org.apache.flink.table.sources.tsextractors.TimestampExtractor

// This is a hack to access Expression.resultType
class IsoDateStringAwareExistingField(val field: String) extends TimestampExtractor {
  override def getArgumentFields: Array[String] = Array(field)

  override def validateArgumentFields(argumentFieldTypes: Array[TypeInformation[_]]): Unit = {
    val fieldType = argumentFieldTypes(0)

    fieldType match {
      case Types.LONG => // OK
      case Types.SQL_TIMESTAMP => // OK
      case Types.STRING => // OK
      case _: TypeInformation[_] =>
        throw ValidationException(
          s"Field '$field' must be of type Long or Timestamp or String but is of type $fieldType.")
    }
  }

  override def getExpression(fieldAccesses: Array[ResolvedFieldReference]): Expression = {
    val fieldAccess: Expression = fieldAccesses(0)

    fieldAccess.resultType match {
      case Types.LONG =>
        // access LONG field
        fieldAccess
      case Types.SQL_TIMESTAMP =>
        // cast timestamp to long
        Cast(fieldAccess, Types.LONG)
      case Types.STRING =>
        Cast(Cast(fieldAccess, SqlTimeTypeInfo.TIMESTAMP), Types.LONG)
    }
  }
}