import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._

// TODO: datasets not working at the moment
// case class User(user_id: Int, first_name: String, last_name: String)
// case class Thing(thing_id: Int, user_id: Int, thing: String)
// case class UsersThings(user_id: Int, first_name: String, last_name: String, things: List[Thing])

object CsvToElasticsearch {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CSV to Elasticsearch")

    conf.set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)
    val sqlc = new org.apache.spark.sql.SQLContext(sc)
    import sqlc.implicits._

    val baseDir = "/user/eric"
    val usersCSVFile = baseDir + "/users.csv"
    val thingsCSVFile = baseDir + "/things.csv"

    val users = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(usersCSVFile)
      // .as[User]

    val things = sqlc.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(thingsCSVFile)
      // .as[Thing]

    val users_things = users
      .join(things, "user_id")
      .groupBy("user_id")
      .agg(
        first($"first_name").as("first_name"), first($"last_name").as("last_name"),
        collect_list(struct("thing_id", "thing")).as("things")
      )

    users_things.saveToEs("users_things/user_thing")

    sc.stop()
  }
}
