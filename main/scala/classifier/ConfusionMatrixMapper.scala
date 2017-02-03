package classifier

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.AccumulatorParam

import org.apache.hadoop.fs.{FileSystem, Path}

import core.{FuzzyRule, Mediator, KnowledgeBase, Variable}

/**
 * Mapper class that classifies each example and returns a confusion matrix
 * @author Eva M. Almansa
 * @version 1.0
 */
class ConfusionMatrixMapper() extends Serializable{
   
  /**
   * Variables for each one map 
   */
  var fuzzyReasoningMethod: Byte = 0
	
  private def broadcastRuleBase(ruleBase: KnowledgeBase, sc: SparkContext) = sc.broadcast(ruleBase)
	
	def setup(conf: SparkConf): ConfusionMatrixMapper = {
		
    //Starting logger
    var logger = Logger.getLogger(this.getClass())    
    
		// Read configuration
		try {
			//Mediator.setConfiguration(conf)
			Mediator.readClassifierConfiguration(conf)
			fuzzyReasoningMethod = Mediator.getFRM()
			//println("fuzzyReasoningMethod="+fuzzyReasoningMethod)
		}
		catch{
			case e: Exception => {
	      System.err.println("\nHITS MAPPER: ERROR READING CONFIGURATION\n")
        e.printStackTrace()
        System.exit(-1)}
		}
		
		this
	}
	
	def mapPartition(values: Iterator[String], ruleBaseBC: Broadcast[KnowledgeBase]): Iterator[Array[Array[Int]]]/*Iterator[Array[IntArrayWritable]]*/ = {
	  
	  val nClasses = ruleBaseBC.value.getDataBase().getNumClasses()
	  val posClassLabels = ruleBaseBC.value.getDataBase().getPosClassLabels()
    var confusionMatrix = Array.fill(nClasses, nClasses)(0)
	  
		// Get the example and the class
	  while(values.hasNext){
      val value = values.next
  	  val input = value.replaceAll(" ", "").split(",")
  	  val example = input.slice(0, input.length -1)
  	  var predictedClass, exampleClass: Byte = 0
  	  
      exampleClass = ruleBaseBC.value.getDataBase().getClassIndex(input(posClassLabels))
      if(exampleClass != -1){
        // Classify the example
        predictedClass = ruleBaseBC.value.classify(fuzzyReasoningMethod, example)
        confusionMatrix(exampleClass)(predictedClass) = confusionMatrix(exampleClass)(predictedClass) + 1
      }
	  }
    
    Iterator(confusionMatrix)
   
  }
	
}

/**
 * Distributed RulesGenerationCS class.
 *
 * @author Eva M. Almansa 
 */
object ConfusionMatrixMapper {
  /**
   * Initial setting necessary.
   */
  def setup(conf: SparkConf) = {
    new ConfusionMatrixMapper().setup(conf)
  }
}
