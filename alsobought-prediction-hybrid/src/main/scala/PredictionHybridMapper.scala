import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import scala.util.control.Breaks._

/**
 * Created by kxhitiz on 5/12/15.
 */
class PredictionHybridMapper extends Mapper[Object,Text,IntPair,DoubleWritable] {
  val one = new DoubleWritable(1.0)

  override
  def map(key:Object, value:Text, context:Mapper[Object,Text,IntPair,DoubleWritable]#Context) = {
    val tokens = value.toString().split("\\s")
    for (token <- 0 until tokens.length - 1) {
      breakable {
        for (nextToken <- token + 1 until tokens.length) {
          if (tokens(token).equals(tokens(nextToken))) {
            break
          }
          val pair         = new IntPair(new IntWritable(tokens(token).toInt), new IntWritable(tokens(nextToken).toInt))
          context.write(pair, one)
        }
      }
    }
  }
}


