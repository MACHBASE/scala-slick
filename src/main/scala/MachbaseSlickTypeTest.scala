import java.sql.Timestamp
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import slick.jdbc.MachbaseProfile.api._
import slick.lifted.TableQuery

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


object MachbaseSlickTypeTest extends App {

    class TypeTestTable(tag: Tag) extends Table[(String, Timestamp, Short, Int, Long, Float, Char, Boolean, Date)](tag, "TYPES") {
        def str_test = column[String]("STR_TEST")
        def date_test = column[Timestamp]("DATETIME_TEST")
        def short_test = column[Short]("SHORT_TEST")
        def int_test = column[Int]("INT_TEST")
        def long_test = column[Long]("LONG_TEST")
        def float_test = column[Float]("FLOAT_TEST")
        def char_test = column[Char]("CHAR_TEST")
        def boolean_test = column[Boolean]("BOOLEAN_TEST")
        def dat_test = column[Date]("DATE_TEST")
        def * = (str_test, date_test, short_test, int_test, long_test,
                float_test, char_test, boolean_test, dat_test)
    }

    val confString =
      """
        | machbase {
        |    dataSourceClass = "slick.jdbc.DatabaseUrlDataSource"
        |    properties {
        |      url = "jdbc:machbase://127.0.0.1:5656/mhdb"
        |      user = "SYS"
        |      password = "MANAGER"
        |    }
        |    numThreads = 1
        |    connectionTimeout = 10000
        | }
      """.stripMargin

    // loading jdbc driver
    Class.forName("com.machbase.jdbc.driver")

    // create db connection
    val conf = ConfigFactory.parseString(confString)
    val db = Database.forConfig("machbase", conf)

    val types = TableQuery[TypeTestTable]

    ////////////////////////////////
    // create tables
    println("Creating table TYPES.")
    val createSchemas = Seq(
        types
    )
    val createTable: DBIOAction[Unit, NoStream, Effect.Schema]
        = DBIO.seq(createSchemas.map(table => table.schema.create): _*)
    val createTableFuture = db.run(createTable)
    Await.result(createTableFuture, 5 seconds)
    println("Table TYPES created.")

    ////////////////////////////////
    /// INSERT
    val utildate = new java.util.Date()
    val datetime = new java.sql.Timestamp(utildate.getTime)
    val sdate = new java.sql.Date(utildate.getTime)
    val ins = DBIO.seq(
        types ++= Seq(
            ("Temperature1", datetime, 1, 1, 1, 1.0f, 'a', true, sdate),
            ("Pressure1", datetime, 2, 2, 2, 2.0f, 'b', false, sdate),
            ("Temperature2", datetime, 3, 3, 3, 3.0f, 'c', true, sdate),
            ("Temperature3", datetime, 4, 4, 4, 4.0f, 'd', false, sdate),
            ("Pressure2", datetime, 5, 5, 5, 5.0f, 'e', true, sdate),
        )
    )

    println("Inserting 5 rows.")

    val insexec = db.run(ins)
    Await.result(insexec, 2 seconds)
    /// INSERT
    println("Insert success.")

    println("Select all records from TYPES.")
    // query all records from TYPEs table
    val queryFuture = db.run(types.result).map(_.foreach {
        case (str_test, date_test, short_test, int_test, long_test, float_test, char_test, boolean_test, sdate) =>
            println(" " + str_test + "\t" + date_test + "\t" + sdate + "\t" + short_test+ "\t" + int_test + "\t" + long_test+ "\t" + float_test + "\t" + char_test + "\t" + "\t" + boolean_test)
    })
    Await.result(queryFuture, 2 seconds)

    println("Select success.")

    println("Select all records from TYPES order by SHORT_TEST.")
    // query all records from TYPEs table
    val queryOrder = db.run(types.sortBy(_.short_test).result).map(_.foreach {
        case (str_test, date_test, short_test, int_test, long_test, float_test, char_test, boolean_test, sdate) =>
            println(" " + str_test + "\t" + date_test + "\t" + sdate + "\t" + short_test+ "\t" + int_test + "\t" + long_test+ "\t" + float_test + "\t" + char_test + "\t" + "\t" + boolean_test)
    })
    Await.result(queryOrder, 2 seconds)

    println("Select success.")

    println("Select records from TYPES where boolean_test is true.")
    // query with where
    // SELECT * FROM types WHERE boolean_test = true
    val queryFilter = db.run(types.filter(_.boolean_test === true).result).map(_.foreach {
        case (str_test, date_test, short_test, int_test, long_test, float_test, char_test, boolean_test, sdate) =>
            println(" " + str_test + "\t" + date_test + "\t" + sdate + "\t" + short_test+ "\t" + int_test + "\t"
              + long_test+ "\t" + float_test + "\t" + char_test + "\t" + "\t" + boolean_test)
    })
    Await.result(queryFilter, 2 seconds)

    println("Select count from TYPES.")

    println("Select count(*) from TYPES.")
    // query all records from TYPEs table
    val querycnt = db.run(types.length.result).map( {
        case (numrow) =>     println("\t\tCount is "+numrow)
    })
    Await.result(querycnt, 2 seconds)

    println("Select avg(float_test) from TYPES.")
    // query avg value for float_test
    val queryavg = db.run(types.map(_.float_test).avg.result).map( {
        case (avgv) =>     println("\t\tAvg is "+ avgv)
    })
    Await.result(queryavg, 2 seconds)

    ////////////////////////////////
    /// DROP TABLE

    println("Drop table types.")
    val dropTable : DBIOAction[Unit, NoStream, Effect.Schema] = DBIO.seq(createSchemas.map(table => table.schema.drop): _*)
    val dropTableFuture = db.run(dropTable)

    Await.result(dropTableFuture, 10 seconds)
    println("Drop success.")
    // disconnect
    db.close()
    println("Disconnected.")
}
