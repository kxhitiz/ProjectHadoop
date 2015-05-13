import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by kxhitiz on 5/12/15.
 */

class PredictionHybridReducer extends Reducer[IntPair,DoubleWritable, IntWritable, Text] {

  var map = scala.collection.Map[IntWritable, scala.collection.mutable.ResizableArray[IntPair]]()
  var sum : Double = 0.0
  val stripe = scala.collection.mutable.Map[String, Double]()
  var bufferKey : IntWritable = null
  var prevValue : Int = 0

  override

  // input: pair (1, 2) => [1,1,1]
  // output: ( 1, [ (2, 2/3), (4, 3/3) ] )
  def reduce(key:IntPair, values:java.lang.Iterable[DoubleWritable], context:Reducer[IntPair,DoubleWritable,IntWritable,Text]#Context) = {

    if (prevValue == 0) {
      prevValue = key.getFirst().get()
    }
    else if (!key.getFirst().get().equals(prevValue)) {
//       println(s"next reducer ${prevValue} == ${key.getFirst().get()}")
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

//    println(s"adding (${prevValue}, (${neighourKey}, ${value+partialSum}))")
    stripe += neighourKey.toString -> (value + partialSum)
//    stripe.foreach{ case (k : String, v : Double) =>
//      println(s"    || (${k}, ${v})")
//    }
  }

  override def cleanup(context:Reducer[IntPair,DoubleWritable,IntWritable,Text]#Context): Unit = {
    stripe.foreach{ case (k : String, v : Double) =>
      stripe += k -> v / sum
    }
    context.write(new IntWritable(prevValue.toInt), new Text(stripe.toString().replaceAll("Map", "")) )
  }

}
