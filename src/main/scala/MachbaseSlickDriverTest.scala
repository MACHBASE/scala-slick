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


object MachbaseSlickDriver extends App {

    class Tags(tag: Tag) extends Table[(String, Timestamp, Double)](tag, "TAG_DATA") {
        def name = column[String]("TAG_NAME")
        def dt = column[Timestamp]("DT")
        def value = column[Double]("VALUE")
        def * = (name, dt, value)
    }

    val confString =
      """
        | machbase {
        |    dataSourceClass = "slick.jdbc.DatabaseUrlDataSource"
        |    properties {
        |      url = "jdbc:machbase://127.0.0.1:8086/mhdb"
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

    val tags = TableQuery[Tags]

    ////////////////////////////////
    // create tables
    println("Creating table tag_data.")
    val createSchemas = Seq(
        tags
    )
    val createTable: DBIOAction[Unit, NoStream, Effect.Schema] = DBIO.seq(createSchemas.map(table => table.schema.create): _*)
    val createTableFuture = db.run(createTable)
    Await.result(createTableFuture, 5 seconds)
    println("Table tag_data created.")

    ////////////////////////////////
    /// INSERT
    val utildate = new java.util.Date()
    val datetime = new java.sql.Timestamp(utildate.getTime)
    val ins = DBIO.seq(
        tags ++= Seq(
            ("Temperature1", datetime, 17.99),
            ("Pressure1", datetime, 8.99),
            ("Temperature2", datetime, 19.99),
            ("Temperature3", datetime, 18.99),
            ("Pressure2", datetime, 9.99)
        )
    )

    println("Inserting 5 rows.")

    val insexec = db.run(ins)
    Await.result(insexec, 2 seconds)
    /// INSERT
    println("Insert success.")

    println("Select all records from tag_data.")
    // query all records from Tag_datas table
    val queryFuture = db.run(tags.result).map(_.foreach {
        case (name, dt, value) =>
            println(" " + dt + "\t" + name + "\t" + value)
    })
    Await.result(queryFuture, 2 seconds)

    println("Select success.")
    ////////////////////////////////
    /// DROP TABLE

    println("Drop table tag_data.")
    val dropTable : DBIOAction[Unit, NoStream, Effect.Schema] = DBIO.seq(createSchemas.map(table => table.schema.drop): _*)
    val dropTableFuture = db.run(dropTable)

    Await.result(dropTableFuture, 10 seconds)
    println("Drop success.")
    // disconnect
    db.close()
    println("Disconnected.")
}
