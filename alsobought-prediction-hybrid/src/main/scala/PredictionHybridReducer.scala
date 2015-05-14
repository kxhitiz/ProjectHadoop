import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by kxhitiz on 5/12/15.
 */

class PredictionHybridReducer extends Reducer[IntPair,DoubleWritable, IntWritable, Text] {

  var sum : Double = 0.0
  val stripe = scala.collection.mutable.Map[String, Double]()
  var prevValue : Int = 0

  override

  def reduce(key:IntPair, values:java.lang.Iterable[DoubleWritable], context:Reducer[IntPair,DoubleWritable,IntWritable,Text]#Context) = {

    if (prevValue == 0) {
      prevValue = key.getFirst().get()
    }
    else if (!key.getFirst().get().equals(prevValue)) {
       stripe.foreach{ case (k : String, v : Double) =>
        stripe += k -> v / sum
      }
      context.write(new IntWritable(prevValue.toInt), new Text(stripe.toString().replaceAll("Map", "")) )
      stripe.clear()
      prevValue = key.getFirst().get()
      sum = 0.0
    }

    val neighourKey = key.getSecond()
    val value = stripe.getOrDefault(neighourKey, 0)

    val partialSum = values.foldLeft(0.0) {(state, elem) => state + elem.get}
    sum += partialSum

    stripe += neighourKey.toString -> (value + partialSum)
  }

  override def cleanup(context:Reducer[IntPair,DoubleWritable,IntWritable,Text]#Context): Unit = {
    stripe.foreach{ case (k : String, v : Double) =>
      stripe += k -> v / sum
    }
    context.write(new IntWritable(prevValue.toInt), new Text(stripe.toString().replaceAll("Map", "")) )
  }

}
