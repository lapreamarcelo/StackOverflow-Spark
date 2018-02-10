import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object StackOverflow {
    
  val sparkConf = new SparkConf().setAppName("StackOverflow")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  /** Usage: StackOverflow [file] */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: StackOverflow <file>")
      System.exit(1)
    }
      
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Read the csv
    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "row").load("hdfs:////stackoverflow//Users.xml")
    df.cache
    df.registerTempTable("table")
    
    //Functions
    getLocations()
    getUsersByYear()
    getCommonNames()
    getVenezuelansUpVotes()
    getCountriesUpVotes()
    getUpVotesByAge()
    sc.stop()
  }
    
  //FUNCTIONS
    
  //CONSULT 1: Top 10 countries in StackOverflow
  //1. Get all the locations with SQL. 
  //2. Then with map we create a tuple by locations 
  //3. Reduce it by key and print the first 10 locations sorted by locations DESC
  def getLocations(){
    println("Top 10 countries in StackOverflow:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _Location from table WHERE _Location NOT LIKE ''")
    val location = sql.rdd.map(x => (x, 1)).reduceByKey(_ + _).sortBy(x => -x._2)
    location.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
    
  //CONSULT 2: New users created by year
  //1. Get all the creationDates with SQL. 
  //2. Then with map we split the date by "-" and get the first element, that is the year 
  //3. Then create a tuple by the year with a map and then reduce it by key and print them all
  def getUsersByYear(){
    println("New users created by year:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _CreationDate from table")
    val dates = sql.rdd.map(x => x.getString(0)).map(x => x.split("-")(0))
    val groupDates = dates.map(x => (x, 1)).reduceByKey(_ + _)
    groupDates.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
    
  //CONSULT 3: Most common names in StackOverflow
  //1. Get all the DisplayNames with SQL. 
  //2. Then with map we create a tuple by DisplayName 
  //3. Reduce it by key and print the first 10 DisplayName most common
  def getCommonNames(){
    println("Most common names in StackOverflow:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _DisplayName from table where _DisplayName IS NOT NULL")
    val names = sql.rdd.map(x => (x, 1)).reduceByKey(_ + _).sortBy(x => -x._2)
    names.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
    
  //CONSULT 4: Venezuelan with more positive votes
  //1. Get all the DisplayNames and UpVotes for Venezuelans only with SQL. 
  //2. Then with map we format the string that we are returning and print the first 10 
  def getVenezuelansUpVotes(){
    println("Venezuelan with more positive votes:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _DisplayName, _UpVotes from table WHERE _Location = 'Venezuela' ORDER BY _UpVotes DESC")
    val venezolanos = sql.rdd.map(x => x(0) + "," + x(1))
    venezolanos.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
    
  //CONSULT 5: Countries with more UpVotes
  //1. Get all the Locations and UpVotes and group it by Locations only with SQL. 
  //2. Then with map we format the string that we are returning and print the first 10
  def getCountriesUpVotes(){
    println("Countries with more UpVotes:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _Location, SUM(_UpVotes) from table WHERE _Location NOT LIKE '' GROUP BY _Location ORDER BY SUM(_UpVotes) DESC")
    val upVotes = sql.rdd.map(x => (x(0) + "," + x(1)))
    upVotes.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
    
  //CONSULT 6: UpVotes groupBy ages
  //1. Get all the Ages and UpVotes and group it by Ages only with SQL. 
  //2. Then with map we format the string that we are returning and print the first 10
  def getUpVotesByAge(){
    println("UpVotes groupBy ages:")
    val startTime = System.nanoTime
    val sql = sqlContext.sql("SELECT _Age, SUM(_UpVotes) from table WHERE _Age IS NOT NULL GROUP BY _Age ORDER BY SUM(_UpVotes) DESC")
    val upVotes = sql.rdd.map(x => (x(0) + "," + x(1)))
    upVotes.take(10).foreach(println)
    val stopTime = System.nanoTime
    val time = (stopTime - startTime) / 1000000000.0
    println("Executed time: "+time+" seconds")
  }
}