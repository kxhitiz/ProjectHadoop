import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by kxhitiz on 5/12/15.
 */

class PredictionHybridReducer extends Reducer[IntPair,DoubleWritable, IntWritable, Text] {

  var map = scala.collection.mutable.Map[IntWritable, scala.collection.mutable.ResizableArray[IntPair]]()
  var sum : Double = 0.0
  var stripe = scala.collection.mutable.Map[IntWritable, Double]()
  var bufferKey : IntWritable = null
  var prevValue : IntWritable = new IntWritable(0)

  override

  // input: pair (1, 2) => [1,1,1]
  // output: ( 1, [ (2, 2/3), (4, 3/3) ] )
  def reduce(key:IntPair, values:java.lang.Iterable[DoubleWritable], context:Reducer[IntPair,DoubleWritable,IntWritable,Text]#Context) = {


//    if (bufferKey != null ) {
//      context.write(key.getFirst(), new Text(bufferKey.toString()))
//    }

    if (bufferKey == null) {
//      context.write(key.getFirst(), new Text("Initial"))
      bufferKey = key.getFirst()
    }
    else if (key.getFirst() != bufferKey) {
//      context.write(key.getFirst(), new Text("Not Equal"))

//      stripe = divideStripeBySum(stripe, sum)

      context.write(bufferKey, new Text(stripe.toString()) )

      stripe.clear()
      bufferKey = key.getFirst()
      sum = 0.0
    }

    val thisVal = key.getSecond()
    val value = stripe.getOrDefault(thisVal, 0)

    val partialSum = values.foldLeft(0.0) {(state, elem) => state + elem.get}
    sum += partialSum

    if (value == 0) {
      context.write(key.getFirst(), new Text("value o"))
      stripe.put(thisVal, partialSum)
    }
    else {
      context.write(key.getFirst(), new Text("value else"))
      stripe.remove(thisVal)
      stripe.put(thisVal, value + partialSum)
    }

    context.write(key.getFirst(), new Text(stripe.toString() + ':' + prevValue.toString))
    prevValue = key.getFirst()
  }
}
