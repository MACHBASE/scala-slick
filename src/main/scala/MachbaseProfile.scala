package slick.jdbc

import java.sql.{PreparedStatement, ResultSet}

import org.slf4j.LoggerFactory
import slick.SlickException
import slick.ast.TypeUtil.:@
import slick.ast._
import slick.basic.Capability
import slick.compiler.{CompilerState, Phase}
import slick.dbio._
import slick.jdbc.meta.MTable
import slick.lifted._
import slick.relational.{CompiledMapping, RelationalCapabilities, RelationalProfile}
import slick.sql.SqlCapabilities
import slick.util.MacroSupport.macroSupportInterpolation

import scala.collection.mutable.Builder
import scala.concurrent.ExecutionContext
import slick.jdbc.meta.{MColumn, MPrimaryKey, MTable}

/*
  Slick profile for Machbase time-series DB.
*/
trait MachbaseProfile extends JdbcProfile {

  private val logger = LoggerFactory.getLogger("MachbaseProfile")

  override protected def computeCapabilities: Set[Capability] = (super.computeCapabilities

    - RelationalCapabilities.functionDatabase
    - RelationalCapabilities.functionUser
    - RelationalCapabilities.reverse
    - RelationalCapabilities.joinFull
    - RelationalCapabilities.foreignKeyActions
    - RelationalCapabilities.functionDatabase
    - RelationalCapabilities.functionUser
    - RelationalCapabilities.repeat
    - RelationalCapabilities.setByteArrayNull
    - RelationalCapabilities.typeBigDecimal
    - RelationalCapabilities.typeLong
    - RelationalCapabilities.zip
    - SqlCapabilities.sequence
    - JdbcCapabilities.forceInsert
    - JdbcCapabilities.nullableNoDefault
    - JdbcCapabilities.mutable
    - JdbcCapabilities.insertOrUpdate
    - JdbcCapabilities.supportsByte
    - JdbcCapabilities.booleanMetaData
    - JdbcCapabilities.returnInsertOther
    - JdbcCapabilities.defaultValueMetaData
    - JdbcCapabilities.distinguishesIntTypes
    - JdbcCapabilities.forUpdate
    )

  override protected lazy val useServerSideUpsert = true
  override protected lazy val useServerSideUpsertReturning = false
  override protected val invokerMutateType: ResultSetType = ResultSetType.ScrollSensitive
  class ModelBuilder(mTables: Seq[MTable], ignoreInvalidDefaults: Boolean)(implicit ec: ExecutionContext) extends JdbcModelBuilder(mTables, ignoreInvalidDefaults) {
    override def createColumnBuilder(tableBuilder: TableBuilder, meta: MColumn): ColumnBuilder = new ColumnBuilder(tableBuilder, meta) {
      /** Regex matcher to extract name and length out of a db type name with length ascription */
      final val TypePattern = "^([A-Z\\s]+)(\\(([0-9]+)\\))?$".r
      private val (_dbType,_size) = meta.typeName match {
        case TypePattern(d,_,s) => (d, Option(s).map(_.toInt))
        case "" => ("TEXT", None)
      }
      override def dbType = Some(_dbType)
      override def length = _size
      override def varying = dbType == Some("VARCHAR")
      override def default = meta.columnDef.map((_,tpe)).collect{
        case ("null",_)  => Some(None) // 3.7.15-M1
        case (v , "java.sql.Timestamp") => {
          import scala.util.{Try, Success}
          val convertors = Seq((s: String) => new java.sql.Timestamp(s.toLong),
            (s: String) => java.sql.Timestamp.valueOf(s),
            (s: String) => new java.sql.Timestamp(javax.xml.bind.DatatypeConverter.parseDateTime(s).getTime.getTime),
            (s: String) => new java.sql.Timestamp(javax.xml.bind.DatatypeConverter.parseDateTime(s.replaceAll(" ","T")).getTime.getTime),
            (s: String) => {
              if(s == "now")
                "new java.sql.Timestamp(java.util.Calendar.getInstance().getTime().getTime())"
              else
                throw new Exception(s"Failed to parse timestamp - $s")
            }
          )
          val v2 = v.replaceAll("\"", "")
          convertors.collectFirst(fn => Try(fn(v2)) match{
            case Success(v) => Some(v)
          })
        }
      }.getOrElse{super.default}
      override def tpe = dbType match {
        case Some("DOUBLE") => "Double"
        case Some("DATE") => "java.sql.Timestamp"
        case Some("BLOB") => "java.sql.Blob"
        case _ => super.tpe
      }
    }
  }

  override def defaultSqlTypeName(tmd: JdbcType[_], sym: Option[FieldSymbol]): String = tmd.sqlType match {
    case java.sql.Types.DATE | java.sql.Types.TIME | java.sql.Types.TIMESTAMP => "DATETIME"
    case java.sql.Types.TINYINT | java.sql.Types.SMALLINT => "SHORT" // Machbase has no smaller binary integer type
    case java.sql.Types.BIGINT => "LONG" // bigint map to long
    case java.sql.Types.FLOAT => "FLOAT" // float map to float
    case _ => super.defaultSqlTypeName(tmd, sym)
  }

  override protected def computeQueryCompiler =
    (super.computeQueryCompiler.addAfter(Phase.removeTakeDrop, Phase.expandSums)
      + Phase.rewriteBooleans)
  override val columnTypes = new JdbcTypes
  override def createQueryBuilder(n: Node, state: CompilerState): QueryBuilder = new QueryBuilder(n, state)
  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)

  override def createColumnDDLBuilder(column: FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)

  override val scalarFrom = Some("sysmachbase.sysdummy1")

  class QueryBuilder(tree: Node, state: CompilerState) extends super.QueryBuilder(tree, state) {

    override protected val hasPiFunction = false
    override protected val hasRadDegConversion = false
    override protected val pi = "3.1415926535897932384626433832"

    override def expr(c: Node, skipParens: Boolean = false): Unit = c match {
      case RowNumber(by) =>
        throw new SlickException("Machbase:Row number function is not supported.")
      case Library.IfNull(l, r) =>
        throw new SlickException("Machbase:IfNull is not supported.")
      case Library.Ceiling(ch) => b"round($ch+0.5)"
      case Library.Floor(ch) => b"round($ch-0.5)"
      case Library.CountAll(LiteralNode(1)) => b"count(*)"
      case _ => super.expr(c, skipParens)
    }

    override protected def buildOrdering(n: Node, o: Ordering) {
      /* Machbase does not have explicit NULLS FIST/LAST clauses. Nulls are
       * sorted after non-null values by default. */
      if(o.nulls.first && !o.direction.desc) {
        b += "case when ("
        expr(n)
        b += ") is null then 0 else 1 end,"
      } else if(o.nulls.last && o.direction.desc) {
        b += "case when ("
        expr(n)
        b += ") is null then 1 else 0 end,"
      }
      expr(n)
      if(o.direction.desc) b += " desc"
    }

    override protected def buildForUpdateClause(forUpdate: Boolean) = {
      super.buildForUpdateClause(forUpdate)
      if(forUpdate) {
        throw new SlickException("Machbase:For update clause is not supported.")
      }
    }
  }

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override protected def createIndex(idx: Index) = {
      if(idx.unique) {
        throw new SlickException("Machbase:Unique index is not supported.")
      } else super.createIndex(idx)
    }

    override def createTable: String = {
      val ddl = super.createTable
      logger.warn(s"Machbase ==> $ddl")
      ddl
    }
  }

  class JdbcTypes extends super.JdbcTypes {
    override val booleanJdbcType = new BooleanJdbcType
    override val charJdbcType = new CharJdbcType
    override val uuidJdbcType = new UUIDJdbcType
    override val floatJdbcType = new FloatJdbcType

    class UUIDJdbcType extends super.UUIDJdbcType {
      override def sqlType = java.sql.Types.CHAR
      override def sqlTypeName(sym: Option[FieldSymbol]) = "CHAR(16) FOR BIT DATA"
    }

    /* Machbase does not have a proper BOOLEAN type. The suggested workaround is
     * a constrained CHAR with constants 1 and 0 for TRUE and FALSE. */
    class BooleanJdbcType extends super.BooleanJdbcType {
      override def sqlTypeName(sym: Option[FieldSymbol]) = "SHORT"
      override def valueToSQLLiteral(value: Boolean) = if(value) "1" else "0"
      override def setValue(v: Boolean, p: PreparedStatement, idx: Int) = if (v) p.setInt(idx, 1) else p.setInt(idx, 0)

      override def getValue(r: ResultSet, idx: Int): Boolean = if (r.getInt(idx) == 0) false else true
    }

    class FloatJdbcType extends super.FloatJdbcType
    {
      override def sqlTypeName(sym:Option[FieldSymbol]) = "FLOAT"
    }
    class CharJdbcType extends super.CharJdbcType
    {
      override def sqlTypeName(sym:Option[FieldSymbol]) = "VARCHAR(1)"
    }
  }

  trait API extends LowPriorityAPI with super.API with ImplicitColumnTypes {
    override type SimpleDBIO[+R] = SimpleJdbcAction[R]
    override val SimpleDBIO = SimpleJdbcAction

    implicit override def queryDeleteActionExtensionMethods[C[_]](q: Query[_ <: RelationalProfile#Table[_], _, C]): DeleteActionExtensionMethods = {
       throw new SlickException("Machbase:Delete statement is not supported.")
    }

    implicit override def runnableCompiledDeleteActionExtensionMethods[RU, C[_]](c: RunnableCompiled[_ <: Query[_, _, C], C[RU]]): DeleteActionExtensionMethods = {
       throw new SlickException("Machbase:Delete statement is not supported.")
    }

    implicit override def runnableCompiledUpdateActionExtensionMethods[RU, C[_]](c: RunnableCompiled[_ <: Query[_, _, C], C[RU]]): UpdateActionExtensionMethods[RU] = {
       throw new SlickException("Machbase:Update statement is not supported.")
    }

    implicit override def jdbcActionExtensionMethods[E <: Effect, R, S <: NoStream](a: DBIOAction[R, S, E]): JdbcActionExtensionMethods[E, R, S] = {
      logger.warn("=============================================")
      new JdbcActionExtensionMethods[E, R, S](a)
    }

    implicit override def actionBasedSQLInterpolation(s: StringContext): ActionBasedSQLInterpolation = {
      logger.warn("=============================================")
      new ActionBasedSQLInterpolation(s)
    }
  }

  override val api: API = new API {}

  override def runSynchronousQuery[R](tree: Node, param: Any)(implicit session: Backend#Session): R = tree match {
    case rsm @ ResultSetMapping(_, _, CompiledMapping(_, elemType)) :@ CollectionType(cons, el) =>
      logger.warn("=============================================")
      val b = cons.createBuilder(el.classTag).asInstanceOf[Builder[Any, R]]
      createQueryInvoker[Any](rsm, param, null).foreach({ x => b += x }, 0)(session)
      b.result()
    case First(rsm: ResultSetMapping) =>
      logger.warn("=============================================")
      createQueryInvoker[R](rsm, param, null).first
  }

}

object MachbaseProfile extends MachbaseProfile