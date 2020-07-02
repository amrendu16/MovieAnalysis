import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical.Join

object movies_analysis {
  def main(args: Array[String]): Unit = {
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  
  val db_table = "directors"
  val driver_lite = "org.sqlite.JDBC"
  val spark = SparkSession
      .builder
      .appName("Movies Reviews")
      .master("local")
      .getOrCreate()
print("Spark Session Created Successfully")
import spark.implicits._

// Details about Director will be stored in Director_details Data Frame
val directors_details = spark.read.format("jdbc").option("url", "jdbc:sqlite:C:\\Users\\amrendu.s1\\workspace\\movies_analysis\\movies.db")
.option("dbtable", db_table)
.option("driver", driver_lite)
.load()

// Details about Movies will be stored in Movies_Details Data Frame
val movies_details = spark.read.format("jdbc").option("url", "jdbc:sqlite:C:\\Users\\amrendu.s1\\workspace\\movies_analysis\\movies.db")
.option("dbtable", "movies")
.option("driver", driver_lite)
.load()

// Details about People will be stored in People_Details Data Frame
val people_details = spark.read.format("jdbc").option("url", "jdbc:sqlite:C:\\Users\\amrendu.s1\\workspace\\movies_analysis\\movies.db")
.option("dbtable", "people")
.option("driver", driver_lite)
.load()

// Details about Ratings will be stored in Rating_Details Data Frame
val ratings_details = spark.read.format("jdbc").option("url", "jdbc:sqlite:C:\\Users\\amrendu.s1\\workspace\\movies_analysis\\movies.db")
.option("dbtable", "ratings")
.option("driver", driver_lite)
.load()

// Details about Stars will be stored in Star_Details Data Frame
val star_details = spark.read.format("jdbc").option("url", "jdbc:sqlite:C:\\Users\\amrendu.s1\\workspace\\movies_analysis\\movies.db")
.option("dbtable", "stars")
.option("driver", driver_lite)
.load()
directors_details.printSchema()
movies_details.printSchema()
people_details.printSchema()
ratings_details.printSchema()
star_details.printSchema()

// Analysis for Top 10 directors who have best average ratings for the movie they directed
// with each movies having atleast 100 votes and with the condition hat such a director has directed atleast
// 4 such movies.
directors_details.join(movies_details, directors_details("movie_id") === movies_details("id") , "inner").
join(ratings_details, directors_details("movie_id") === ratings_details("movie_id"), "inner").
join(people_details, directors_details("person_id") === people_details("id"), "inner").
where(ratings_details("votes") >=100).groupBy(directors_details("person_id"), people_details("name")).
agg(count(movies_details("id")).as("TotalNoOfMoviesDirected"), avg(ratings_details("rating")).as("AverageRatingOfMoviesDirected")).
filter($"TotalNoOfMoviesDirected" >= 4).orderBy(desc("AverageRatingOfMoviesDirected")).
select(directors_details("person_id").alias("director_id"), people_details("name").alias("director_name")
    , $"AverageRatingOfMoviesDirected", $"TotalNoOfMoviesDirected")
.limit(10).show()

// Analysis for year of each category compared to others- 
// Most number of Years- Year in which most movies are produced
// Highest rated year (Year in which avg higest rating of movies is higest)
// Least Number of movies & Lowest Rated Year
val max_movies_year_df= movies_details.join(ratings_details, movies_details("id") === ratings_details("movie_id"), "inner").
                              groupBy(movies_details("year")).agg(count(movies_details("id")).as("movie_count"))
                              .orderBy(desc("movie_count")).
                              select(movies_details("year").as("year")).withColumn("Category", lit("Most Number of Movies")).limit(1)
                              
val min_movies_year_df= movies_details.join(ratings_details, movies_details("id") === ratings_details("movie_id"), "inner").
                              groupBy(movies_details("year")).agg(count(movies_details("id")).as("movie_count"))
                              .orderBy(asc("movie_count")).
                              select(movies_details("year").as("year")).withColumn("Category", lit("Least Number of Movies")).limit(1)
                                                            
val higest_rated_year_df = movies_details.join(ratings_details, movies_details("id") === ratings_details("movie_id"), "inner").
                               groupBy(movies_details("year")).agg(avg(ratings_details("rating")).as("average_rating"))
                               .orderBy(desc("average_rating"))
                               .select(movies_details("year").as("year")).withColumn("Category", lit("Higest Rated Year")).limit(1)
                               
val lowest_rated_year_df = movies_details.join(ratings_details, movies_details("id") === ratings_details("movie_id"), "inner").
                               groupBy(movies_details("year")).agg(avg(ratings_details("rating")).as("average_rating"))
                               .orderBy(asc("average_rating"))
                               .select(movies_details("year").as("year")).withColumn("Category", lit("Lowest Rated Year")).limit(1)
  
// Final Result for Problem Statement 2
max_movies_year_df.union(higest_rated_year_df).union(min_movies_year_df).union(lowest_rated_year_df).show()                             

  }
}
