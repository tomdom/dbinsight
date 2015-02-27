package com.github.tomdom.dbinsight

import com.github.tomdom.scalabase.Helper._

import java.sql.{ SQLException, ResultSet, DatabaseMetaData, Connection }

import org.mockito
import org.mockito.Mockito._
import org.mockito.Matchers._

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, FlatSpec }

class DBInsightMockSpec extends FlatSpec with MockitoSugar with Matchers {
  case class Catalog(name: String)
  case class Table(name: String)
  case class Column(name: String)

  "DBInsight" should "stream catalogs (internal ResultSet.getString not called)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(1)) thenReturn "catalog_1" thenReturn "catalog_2" thenReturn "catalog_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getCatalogs) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    //    implicit def toCatalog(rs: ResultSet) = Catalog(rs.getString(1))
    using(DBInsight.catalogsData(c)) { cns =>
      verify(rs, times(0)).getString(1)
    }
  }

  it should "stream catalogs (internal ResultSet.getString called 1 time)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(1)) thenReturn "catalog_1" thenReturn "catalog_2" thenReturn "catalog_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getCatalogs) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toCatalog(rs: ResultSet) = Catalog(rs.getString(1))
    using(DBInsight.catalogsData[Catalog](c)) { cns =>
      val catalogNames: Stream[Catalog] = cns
      verify(rs).getString(1)
    }
  }

  it should "stream catalogs (internal ResultSet.getString called 3 times until finished)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(1)) thenReturn "catalog_1" thenReturn "catalog_2" thenReturn "catalog_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getCatalogs) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toCatalog(rs: ResultSet) = Catalog(rs.getString(1))
    using(DBInsight.catalogsData[Catalog](c)) { cns =>
      val catalogNames: Stream[Catalog] = cns
      val cnwis = catalogNames.zipWithIndex.map(t => t._1 -> (t._2 + 1))
      for (cnwi <- cnwis) {
        cnwi._1.name shouldBe "catalog_" + cnwi._2
        verify(rs, times(cnwi._2)).getString(1)
      }
      verify(rs, times(3)).getString(1)
    }
  }

  it should "stream tables (internal ResultSet.getString not called)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(3)) thenReturn "table_1" thenReturn "table_2" thenReturn "table_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getTables(null, null, null, null)) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toTable(rs: ResultSet) = Table(rs.getString(3))
    using(DBInsight.tablesData[Table](c)) { cns =>
      verify(rs, times(0)).getString(1)
    }
  }

  it should "stream tables (internal ResultSet.getString called 1 time)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(3)) thenReturn "table_1" thenReturn "table_2" thenReturn "table_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getTables(null, null, null, null)) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toTable(rs: ResultSet) = Table(rs.getString(3))
    using(DBInsight.tablesData[Table](c)) { cns =>
      val tableNames: Stream[Table] = cns
      verify(rs).getString(3)
    }
  }

  it should "stream tables (internal ResultSet.getString called 3 times until finished)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(3)) thenReturn "table_1" thenReturn "table_2" thenReturn "table_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getTables(null, null, null, null)) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toTable(rs: ResultSet) = Table(rs.getString(3))
    using(DBInsight.tablesData[Table](c)) { cns =>
      val tableNames: Stream[Table] = cns
      val tnwis = tableNames.zipWithIndex.map(t => t._1 -> (t._2 + 1))
      for (tnwi <- tnwis) {
        tnwi._1.name shouldBe "table_" + tnwi._2
        verify(rs, times(tnwi._2)).getString(3)
      }
      verify(rs, times(3)).getString(3)
    }
  }

  it should "stream columns (internal ResultSet.getString not called)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(4)) thenReturn "column_1" thenReturn "column_2" thenReturn "column_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getColumns(any(), any(), anyString(), any())) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toColumn(rs: ResultSet) = Column(rs.getString(4))
    using(DBInsight.columnsData[Column](c, "tableName")) { cns =>
      verify(rs, times(0)).getString(4)
    }
  }

  it should "stream columns (internal ResultSet.getString called 1 time)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(4)) thenReturn "column_1" thenReturn "column_2" thenReturn "column_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getColumns(any(), any(), anyString(), any())) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toColumn(rs: ResultSet) = Column(rs.getString(4))
    using(DBInsight.columnsData[Column](c, "tableName")) { cns =>
      val columnNames: Stream[Column] = cns
      verify(rs).getString(4)
    }
  }

  it should "stream columns (internal ResultSet.getString called 3 times until finished)" in {
    val rs = mock[ResultSet]
    when(rs.next()) thenReturn true thenReturn true thenReturn true thenReturn false
    when(rs.getString(4)) thenReturn "column_1" thenReturn "column_2" thenReturn "column_3" thenThrow new SQLException

    val metaData = mock[DatabaseMetaData]
    when(metaData.getColumns(any(), any(), anyString(), any())) thenReturn rs

    val c = mock[Connection]
    when(c.getMetaData) thenReturn metaData

    implicit def toColumn(rs: ResultSet) = Column(rs.getString(4))
    using(DBInsight.columnsData[Column](c, "tableName")) { cns =>
      val columnNames: Stream[Column] = cns
      val cnwis = columnNames.zipWithIndex.map(t => t._1 -> (t._2 + 1))
      for (cnwi <- cnwis) {
        cnwi._1.name shouldBe "column_" + cnwi._2
        verify(rs, times(cnwi._2)).getString(4)
      }
      verify(rs, times(3)).getString(4)
    }
  }
}
