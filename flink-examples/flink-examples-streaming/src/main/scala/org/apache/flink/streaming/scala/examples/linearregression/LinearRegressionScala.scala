import java.lang.Iterable

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.util.Collector
import org.apache.flink.examples.java.ml.util.LinearRegressionData
import java.util.logging._

import scala.util.Random
import scala.collection.JavaConversions._

object LinearRegressionScala {
  protected val LOG: Logger = Logger.getLogger(getClass.getName)
  val accuracy = 0.01


  def main (args: Array[String]){
    //val LOG =   Logger.getLogger("MyLog")
    val fh = new FileHandler("/home/kiss/log/streamOrig.log")
    LOG.addHandler(fh);
    val formatter = new SimpleFormatter();
    fh.setFormatter(formatter);

    // the following statement is used to log any messages
    LOG.info("Linear regression - original streaming algorithm");



    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val typedList : Array[Either[Data, Params]] = LinearRegressionData.DATA
      .map(pair => Data(pair(0).toString.toDouble, pair(1).toString.toDouble))
      .map(Left(_))

    val data = env.fromCollection(typedList)
      .map(x => x)
      .rebalance

    val iteration = data.iterate{data =>
      val updated = data.flatMap(new SubUpdate)
        .flatMap(new UpdateAccumulator).setParallelism(1)
        .map(new Update)
        .broadcast

      val connected = data.connect(updated)
        .map(x => x, x => Right(x))
        .split(new IterationSelector)
      (connected.select("iterate"), connected.select("output"))
    }

    iteration print

    env execute
    //  System.out.println(env.getExecutionPlan)
  }

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  /**
    * A simple data sample, x means the input, and y means the target.
    */
  case class Data(var x: Double, var y: Double)

  /**
    * A set of parameters -- theta0, theta1.
    */
  case class Params(theta0: Double, theta1: Double) {
    def div(a: Int): Params = {
      Params(theta0 / a, theta1 / a)
    }
  }

  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
    * Compute a single BGD type update for every parameters.
    */

  class SubUpdate extends RichFlatMapFunction[Either[Data, Params],
    (Params, Int)]{
    private var parameter: Params = Params(0.0, 0.0)
    private val count: Int = 1

    override def flatMap(in: Either[Data, Params],
                         collector: Collector[(Params, Int)]): Unit = {
      println ("Param theta0 :"+parameter.theta0 +" theta1 "+parameter.theta1)
      in match {
        case Left(data) => {
          val theta_0 = parameter.theta0 - 0.01 *
            ((parameter.theta0 + (parameter.theta1 * data.x)) - data.y)
          val theta_1 = parameter.theta1 - 0.01 *
            ((parameter.theta0 + (parameter.theta1 * data.x)) - data.y) * data.x

          collector.collect(Params(theta_0, theta_1), count)
        }
        case Right(param) => {
          //////////////
         // 0.17446349489017998 1.9730931114984227
         // val difference =  ( parameter.theta0 - param.theta0)*(parameter.theta0 - param.theta0) + (parameter.theta1 - param.theta1)*(parameter.theta1 - param.theta1)
          val difference1 =  ( 0.17446349489017998 - param.theta0)*(0.17446349489017998 - param.theta0)
          val difference2 =  (1.9730931114984227 - param.theta1)*(1.9730931114984227 - param.theta1)
          if ((difference1 < 0.001) && (difference2 < 0.001))
            throw new Exception(param.theta0.toString+" "+param.theta1.toString+" "+difference2 )
          LOG.info("Difference :"+difference1 +" " +difference2)
          //////////////
          parameter = param
        }
      }
    }
  }

  class UpdateAccumulator extends FlatMapFunction[(Params, Int),
    (Params, Int)]{
    var value = (Params(0.0, 0.0), 0)

    override def flatMap(param: (Params, Int),
                         collector: Collector[(Params, Int)]) = {

      val new_theta0: Double = param._1.theta0 + value._1.theta0
      val new_theta1: Double = param._1.theta1 + value._1.theta1
      value = (Params(new_theta0, new_theta1), param._2 + value._2)
      LOG.info("Param: "+ value._1.theta0.toString+" "+value._1.theta1.toString+" "+value._2.toString )
      collector.collect(value)

    }
  }

  class Update extends MapFunction[(Params, Int), Params] {
    override def map(param: (Params, Int) ): Params = {
      /////////////
      LOG.info( "Updated parameters: "+ param._1.theta0 / param._2 +" "+ param._1.theta0 / param._2)
      /////////////
      param._1.div(param._2)
    }
  }

  class DataFilter extends FlatMapFunction[Either[Data, Params], Data]{
    override def flatMap(value: Either[Data, Params], out: Collector[Data]): Unit = {
      value match {
        case Left(_) => out.collect(value.left.get)
        case Right(_) =>
      }
    }
  }

  class IterationSelector extends OutputSelector[Either[Data, Params]] {
    @transient
    var rnd : Random = null

    override def select(value: Either[Data, Params]): Iterable[String] = {
      /*  if (rnd == null) {
       rnd = new Random()
     }
     if (rnd.nextInt(10) < 6) {
       List("output")
     } else {
       List("iterate")
     }*/
      List("iterate")
    }
  }
}
