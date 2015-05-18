import domains.MongoConnector
import org.apache.spark.SparkContext
import SparkContext._
import reactivemongo.bson.{BSONInteger, BSONDocument}

/**
 * Created by prayagupd
 * on 5/16/15.
 */

class Recommender (@transient sc : SparkContext) extends Serializable {

  /**
   * Parameters to regularize correlation.
   */
  val PRIOR_COUNT = 10
  val PRIOR_CORRELATION = 0

  val TRAIN_FILENAME = "hdfs://localhost:9000/user/kxhitiz/ua.base"
  val TEST_FIELNAME = "hdfs://localhost:9000/user/kxhitiz/ua.test"
  val MOVIES_FILENAME = "hdfs://localhost:9000/user/kxhitiz/u.item"

  @transient val db = new MongoConnector()


  def predict(movieName : String): Unit = {
    db.list(movieName)
  }

  def init (): Unit = {

    println(s"db ${db}")
    db.connect()

    // get movie names keyed on id
    val movies = sc.textFile(MOVIES_FILENAME)
      .map(line => {
      val fields = line.split("\\|")
      (fields(0).toInt, fields(1))
    })

    val movieNames = movies.collectAsMap()    // for local use to map id <-> movie name for pretty-printing

    // extract (userid, movieid, rating) from ratings data
    val ratingsTrainMap = sc.textFile(TRAIN_FILENAME)
      .map(line => {
      val fields = line.split("\t")
      (fields(0).toInt, fields(1).toInt, fields(2).toInt)
    })

    // get num raters per movie, keyed on movie id
    val numRatersPerMovie = ratingsTrainMap
      .groupBy(tup => tup._2)
      .map(grouped => (grouped._1, grouped._2.size))

    // join ratings with num raters on movie id
    val ratingsWithSize = ratingsTrainMap
      .groupBy(tup => tup._2)
      .join(numRatersPerMovie)
      .flatMap(joined => {
      joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))
    })

    // ratingsWithSize now contains the following fields: (user, movie, rating, numRaters).

    // dummy copy of ratings for self join
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    // join on userid and filter movie pairs such that we don't double-count and exclude self-pairs
    val ratingPairs =
      ratingsWithSize
        .keyBy(tup => tup._1)
        .join(ratings2)
        .filter(f => f._2._1._2 < f._2._2._2)

    // compute raw inputs to similarity metrics for each movie pair
    val vectorCalcs =
      ratingPairs
        .map(data => {
        val key = (data._2._1._2, data._2._2._2)
        val stats =
          (data._2._1._3 * data._2._2._3, // rating 1 * rating 2
            data._2._1._3,                // rating movie 1
            data._2._2._3,                // rating movie 2
            math.pow(data._2._1._3, 2),   // square of rating movie 1
            math.pow(data._2._2._3, 2),   // square of rating movie 2
            data._2._1._4,                // number of raters movie 1
            data._2._2._4)                // number of raters movie 2
        (key, stats)
      })
        .groupByKey()
        .map(data => {
        val key = data._1
        val vals = data._2
        val size = vals.size
        val dotProduct = vals.map(f => f._1).sum
        val ratingSum = vals.map(f => f._2).sum
        val rating2Sum = vals.map(f => f._3).sum
        val ratingSq = vals.map(f => f._4).sum
        val rating2Sq = vals.map(f => f._5).sum
        val numRaters = vals.map(f => f._6).max
        val numRaters2 = vals.map(f => f._7).max
        (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
      })

    // compute similarity metrics for each movie pair
    val similarities =
      vectorCalcs
        .map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val corRelation            = Measures.correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val regularizedCorRelation = Measures.regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
        val cosineSimilarity       = Measures.cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
        val jaccardSimilarity      = Measures.jaccardSimilarity(size, numRaters, numRaters2)

        (key, (corRelation, regularizedCorRelation, cosineSimilarity, jaccardSimilarity))
      })

    //persit to mongo

    println(s"==================================")
    println(s"trying to insert each of similarities ")
    val array = similarities.collect()
    println(s"trying to insert each of similarities ${array.length}")
    array.foreach { case (k, v) =>
      println(s"==================================")
      //println(s"(${k._1}, ${k._2})")
      //      val json = BSONDocument("id" -> 1,
      //        "similarId" -> 2)
      val x = if(v._2.equals(Double.NaN)) 0 else v._2
      val json = BSONDocument("movieId" -> k._1,
        "title" -> movieNames(k._1),
        "similarId" -> k._2,
        "similarTitle" -> movieNames(k._2),
        "regularizedCorRelation" -> x)

      println(s"inserting each doc to ${db}")
      db.insert(json)
      println(s"==================================")
    }

    println(s"after insertion")
    println(s"==================================")
    // test a few movies out (substitute the contains call with the relevant movie name
    //    val sample = similarities.filter(m => {
    //      val movies = m._1
    //      (movieNames(movies._1).contains(movieName))
    //    })
    //
    //    // collect results, excluding NaNs if applicable
    //    val result = sample.map(v => {
    //      val m1 = v._1._1
    //      val m2 = v._1._2
    //      val corr = v._2._1
    //      val rcorr = v._2._2
    //      val cos = v._2._3
    //      val j = v._2._4
    //      (movieNames(m1), movieNames(m2), corr, rcorr, cos, j)
    //    }).collect()
    //      .filter(e => !(e._4 equals Double.NaN))    // test for NaNs must use equals rather than ==
    //      .sortBy(elem => elem._4)
    //      .take(10)
    //
    //    // print the top 10 out
    //    result.foreach(r =>
    //      println(r._1 + " | " + r._2 + " | " + r._3.formatted("%2.4f") + " | " + r._4.formatted("%2.4f") + " | " + r._5.formatted("%2.4f") + " | " + r._6.formatted("%2.4f"))
    //    )

  }
}
