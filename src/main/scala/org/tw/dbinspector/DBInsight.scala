package com.github.tomdom.dbinsight

import java.sql.{ ResultSet, Connection, DatabaseMetaData }

import com.github.tomdom.scalabase.sql.ResultSetOps._

object DBInsight {
  implicit def toStream[T](r: Result[T]): Stream[T] = r.getStream()
  class Result[T](val rs: ResultSet, val toRow: ResultSet => T) {
    def getStream(): Stream[T] = rs.toStream(toRow)

    def close(): Unit = rs.close()
  }

  private def catalogs(c: Connection): ResultSet = c.getMetaData.getCatalogs
  def catalogsData[T](c: Connection)(implicit toRow: ResultSet => T): Result[T] = new Result(catalogs(c), toRow)

  private def tables(c: Connection): ResultSet = c.getMetaData.getTables(null, null, null, null)
  def tablesData[T](c: Connection)(implicit toRow: ResultSet => T): Result[T] = new Result(tables(c), toRow)

  private def columns(c: Connection, tableName: String): ResultSet = c.getMetaData.getColumns(null, null, tableName, null)
  def columnsData[T](c: Connection, tableName: String)(implicit toRow: ResultSet => T): Result[T] = new Result(columns(c, tableName), toRow)
}
