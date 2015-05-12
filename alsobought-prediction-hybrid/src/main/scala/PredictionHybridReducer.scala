import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._

/**
 * Created by kxhitiz on 5/12/15.
 */

class PredictionHybridReducer extends Reducer[IntPair,DoubleWritable, IntPair,DoubleWritable] {

  override
  def reduce(key:IntPair, values:java.lang.Iterable[DoubleWritable], context:Reducer[IntPair,DoubleWritable,IntPair,DoubleWritable]#Context) = {

  }
}
