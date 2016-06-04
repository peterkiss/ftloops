/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
  * This example implements a basic Linear Regression  to solve the y = theta0 + theta1*x problem using batch gradient descent algorithm.
  *
  * <p>
  * Linear Regression with BGD(batch gradient descent) algorithm is an iterative clustering algorithm and works as follows:<br>
  * Giving a data set and target set, the BGD try to find out the best parameters for the data set to fit the target set.
  * In each iteration, the algorithm computes the gradient of the cost function and use it to update all the parameters.
  * The algorithm terminates after a fixed number of iterations (as in this implementation)
  * With enough iteration, the algorithm can minimize the cost function and find the best parameters
  * This is the Wikipedia entry for the <a href = "http://en.wikipedia.org/wiki/Linear_regression">Linear regression</a> and <a href = "http://en.wikipedia.org/wiki/Gradient_descent">Gradient descent algorithm</a>.
  *
  * <p>
  * This implementation works on one-dimensional data. And find the two-dimensional theta.<br>
  * It find the best Theta parameter to fit the target.
  *
  * <p>
  * Input files are plain text files and must be formatted as follows:
  * <ul>
  * <li>Data points are represented as two double values separated by a blank character. The first one represent the X(the training data) and the second represent the Y(target).
  * Data points are separated by newline characters.<br>
  * For example <code>"-0.02 -0.04\n5.3 10.6\n"</code> gives two data points (x=-0.02, y=-0.04) and (x=5.3, y=10.6).
  * </ul>
  *
  * <p>
  * This example shows how to use:
  * <ul>
  * <li> Bulk iterations
  * <li> Broadcast variables in bulk iterations
  * <li> Custom Java objects (PoJos)
  * </ul>
  */

package org.apache.flink.streaming.scala.examples.linearregression

import java.lang.Iterable

import org.apache.flink.api.common.functions._
import org.apache.flink.examples.java.ml.util.LinearRegressionData
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.util.Random

object LinearRegressionScalaModifiedExOnce {
  ////
  var dataCounter = 1
  ////
  def main (args: Array[String]){
    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //LinearRegressionData.DATA -> returns object[][] where obj ={double, double}
    val typedList : Array[Either[Data, Params]] = LinearRegressionData.DATA
      .map(pair => Data(pair(0).toString.toDouble, pair(1).toString.toDouble)) // convert double pairs to DATA
      .map(Left(_)) // typedlist only contains these pairs
    /**
      * StreamExecutionEnvironment.fromCollection
      * Creates a DataStream from the given non-empty [[Seq]]. The elements need to be serializable
      * because the framework may move the elements into the cluster if needed.
      *
      * Note that this operation will result in a non-parallel data source, i.e. a data source with
      * a parallelism of one.
      */
    val data = env.fromCollection(typedList)
      .map(x => x)
      .rebalance
    /**
      * Initiates an iterative part of the program that creates a loop by feeding
      * back data streams. To create a streaming iteration the user needs to define
      * a transformation that creates two DataStreams. The first one is the output
      * that will be fed back to the start of the iteration and the second is the output
      * stream of the iterative part.
      *
      * The input stream of the iterate operator and the feedback stream will be treated
      * as a ConnectedStreams where the the input is connected with the feedback stream.
      *
      * This allows the user to distinguish standard input from feedback inputs.
      *
      * stepfunction: initialStream => (feedback, output)
      *
      * The user must set the max waiting time for the iteration head.
      * If no data received in the set time the stream terminates. If this parameter is set
      * to 0 then the iteration sources will indefinitely, so the job must be killed to stop.
      *
      */


    val iteration = data.iterate{data =>

      //TODO output of subupdate should be collected in 1s windows
      // compute a single step using every sample
        val updated = data.flatMap(new SubUpdate)
          .timeWindowAll(Time.seconds(1))
          // sum up all the steps in window
          .reduce( new UpdateAccumulator2)//.countWindowAll(20).reduce()
          // average the steps and update all parameters

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
  case class Data(var x: Double, var y: Double){var c =  dataCounter+1; dataCounter = c}


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
    * Compute a single BGD (Batch gradient descent) type update for every parameters.
    * how to change theta1 and theta1 of the line
    * counts a modified line after each point and sends those
    */
  //locally executed -> receives either data or param
  class SubUpdate extends RichFlatMapFunction[Either[Data, Params],
    (Params, Int)]{
    private var parameter: Params = Params(0.0, 0.0)
    private val count: Int = 1
    private val learningrate = 0.001
    private val accuracy = 0.1

    override def flatMap(in: Either[Data, Params],
                         collector: Collector[(Params, Int)]): Unit = {
      in match {
        case Left(data) => { // if it receives actual points
          println(data)
          println ("Parameter  = "+ parameter)
          //m_current = theta1
          //b_current = theta0
          //          -(2/N) * (points[i].y - ((m_current*points[i].x) + b_current))
          val delta_0 = - 0.1 * (data.y - (parameter.theta0 + (parameter.theta1 * data.x)) )
          //          -(2/N) * points[i].x * (points[i].y - ((m_current * points[i].x) + b_current))
          val delta_1 = - 0.1 * data.x * (data.y- ( (parameter.theta1 * data.x)+parameter.theta0 ) )
          val parameter1 = new Params(delta_0, delta_1)
          println ("Parameter delta = "+ parameter1)
          collector.collect(parameter1, count)
        }
        case Right(param) => {
          println ("Old Parameter  = "+ parameter)
          println ("Received Parameter delta  = "+ param)
          val difference1 = Math.sqrt( ( parameter.theta0 - 0)*(parameter.theta0 - 0))
          val difference2 = Math.sqrt((parameter.theta1 - 2)*(parameter.theta1 - 2))
          if ((difference1 < accuracy) && (difference2 < accuracy))
          throw new Exception(parameter.theta0.toString+" "+parameter.theta1.toString+" "+difference1+" "+difference2 )
         // println ("Difference :"+difference)
          parameter = new Params(( parameter.theta0-learningrate*param.theta0 ),(parameter.theta1- learningrate*param.theta1 ))
          println ("New parameter = "+parameter)
        }
      }
    }
  }


  class UpdateAccumulator1 extends FoldFunction[(Params, Int),
    (Params, Int)]{
   // var value = (Params(0.0, 0.0), 0)

    override def fold( value :(Params,Int) , param: (Params, Int)) : (Params, Int)= {

      val new_theta0: Double = param._1.theta0 + value._1.theta0
      val new_theta1: Double = param._1.theta1 + value._1.theta1
      val new_value = (Params(new_theta0, new_theta1), param._2 + value._2)
      //println new_value._2
      return new_value;

    }
  }
  /**
  *collects the modified lines sum the parameterds
  */
  class UpdateAccumulator2 extends ReduceFunction[(Params, Int)]{
    // var value = (Params(0.0, 0.0), 0)

    override def reduce( value1 :(Params,Int) , value2: (Params, Int)) : (Params, Int)= {
      println ("reduce Param1 = "+ value1._1)
      println ("reduce Param2 = "+ value2._1)

      val new_theta0: Double = value1._1.theta0 + value2._1.theta0
      val new_theta1: Double = value1._1.theta1 + value2._1.theta1
     // val p1 = Params(new_theta0, new_theta1)
      //val new_value = (Params(new_theta0, new_theta
      //val new_value =( p1.div( value1._2 + value2._2),value1.)
      val new_value = (Params(new_theta0, new_theta1), value2._2 + value1._2)

      println ("REDUCED Param = "+ new_value._1+" Counter = " + new_value._2)
      return new_value;

    }
  }

  class UpdateAccumulator extends FlatMapFunction[(Params, Int),
    (Params, Int)]{
    var value = (Params(0.0, 0.0), 0)

    override def flatMap(param: (Params, Int),
                         collector: Collector[(Params, Int)]) = {
      // add new entries data to value
      val new_theta0: Double = param._1.theta0 + value._1.theta0
      val new_theta1: Double = param._1.theta1 + value._1.theta1
      value = (Params(new_theta0, new_theta1), param._2 + value._2)

      collector.collect(value)

    }
  }

  /**
    *
    */
  class Update extends MapFunction[(Params, Int), Params] {
    override def map(param: (Params, Int) ): Params = {
      println( "UPDATED parameter: "+ param._1.theta0 / param._2 +" "+ param._1.theta1 / param._2 +" Count:  " +param._2)
      param._1///.div(param._2)

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

  //TODO send back everything
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
      //List("output")
    }
  }
}
