package learner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.{BufferedWriter, OutputStreamWriter}

import core.{DataBase, Mediator, FuzzyRule, FuzzyVariable, KnowledgeBase, Population, Variable}
import utils.ConsequentPart

/**
 * Cost-sensitive version of RulesGenerationMapper 
 * @author Eva M. Almansa (eva.m.almansa@gmail.com)
 * @author Alberto Fernandez (alberto@decsai.ugr.es) - University of Granada
 * @version 1.1 (A. Fernandez) - 12/06/2017
 */
class RulesGenerationCS() extends Serializable {
  
  /**
	 * DataBase
	 */
	private var dataBase: DataBase = null
  
	/**
	 * Rule Base (cannot use Array as key, not hashcode available)
	 */
	private var ruleBase: Map[FuzzyRule, Array[ConsequentPart]] = null // Key: antecedents of the rule, Value: Classes of the rule
	private var classCost: Array[Double] = null // Cost associated to each class
	private var rw: Byte = 0;
		
	private var matchingDegrees: Array[Array[Double]] = null;   
  private var membershipDegrees: Array[Array[Double]] = null;

	
	/**
	 * Variables's CHC genetic algorithm (for EFS Rule Selection)
	 */
	var popSize, numEvaluations: Int = 0
	var alpha: Double = 0.0
	
	/**
	 * Time Counters
	 */
	private var startMs, endMs: Long = 0
	
	/**
	 * Temporal time output file
	 */
	private var time_outputFile: String = ""
	
	/**
	 * Dataset
	 */
	private var inputValues: Array[Array[String]] = null // Input values of the instances of the current split of the dataset
	private var classLabels: Array[Byte] = null // Indices of the instances class labels
	
	/**
	 * Info. variables
	 */
	private var counter_class:  Array[Int] = null // Counter the occurrences related with positive class and negative class 
	private var counter_rules:  Array[Int] = null // Counter the occurrences related with positive class and negative class
	
	/**
	 * Compute the membership degree of the current value to all linguistic labels
	 */
	private def computeMatchingDegreesAll(){
	
	  matchingDegrees = Array.fill(ruleBase.size,dataBase.getNumClasses())(0.0)  
    membershipDegrees = Array.fill(dataBase.getNumVariables(), dataBase.getNumLinguisticLabels())(0.0)
    
    var i = 0
    for (input <- inputValues){
      //pre-computation of the membership degrees for the rule weight
      for (j <- 0 to (dataBase.getNumVariables() - 1)){ 
        if (dataBase.get(j).isInstanceOf[FuzzyVariable]){
          for (label <- 0 to (dataBase.getNumLinguisticLabels() - 1)){
            membershipDegrees(j)(label) = dataBase.computeMembershipDegree(j.toByte, label.toByte, input(j))
          }
        }
      }
      var classIndex = classLabels(i)
      var j = 0
      for (rule <- ruleBase){
          //t-norm computation for all rules
          matchingDegrees(j)(classIndex) += dataBase.computeMatchingDegree(membershipDegrees, rule._1.getAntecedent(), input)* classCost(classIndex); 
          //(dataBase.computeMatchingDegree(membershipDegrees, ruleBase(0), input) * classCost(classIndex))
          j+=1
      }
      i+=1
    }//for examples
	}

	private def computeConsequent(consequents: Array[ConsequentPart], ruleId: Int): ConsequentPart = {
	  
		var weight, weightOther, sumTotal: Double = 0.0
      var classIndex,s: Byte = 0
      for(consequent <- consequents){ //for all consequent classes of the rule
        sumTotal = sumTotal + matchingDegrees(ruleId)(consequent.getClassIndex())
      }
      for(consequent <- consequents){ //for all consequent classes of the rule
        var currWeight = matchingDegrees(ruleId)(consequent.getClassIndex());
        weightOther = 0;
        if (rw == KnowledgeBase.RW_PCF){ //just in case of PCF
          weightOther = sumTotal - currWeight; 
        }
        currWeight = (currWeight - weightOther)/sumTotal
        if(currWeight > weight){
            weight = currWeight
            classIndex = consequent.getClassIndex()
          }
      }
		 val cq = new ConsequentPart(classIndex,weight);
		 cq
	} 
	
	/**
	 * Initializes the variables and data structures: learner and databse
	 */
	def setup(sc: SparkContext, conf: SparkConf): RulesGenerationCS = {
    
		//Starting logger
   var logger = Logger.getLogger(this.getClass())    
   logger.info("Starting setup");
    /**
		 * STEP 1: Read Learner configuration (paths, labels, and so on)
		 */
		try {
			popSize = Mediator.getPopSize()
			numEvaluations = Mediator.getNumEvaluations()
			alpha = Mediator.getAlpha()
			time_outputFile = Mediator.getLearnerOutputPath()+"//"+Mediator.TIME_STATS_DIR
			rw = Mediator.getRW()
		}
		catch{
		   case e: Exception => {
        logger.error("\nSTAGE 1: ERROR READING CONFIGURATION => ")
        logger.error(e.toString)
        System.exit(-1)}
		}
		
		/**
		 * STEP 2: Read DataBase configuration (create fuzzy partitions)
		 */
		try{
			dataBase = new DataBase(sc, Mediator.getHeaderPath())
		}catch{
		  case e: Exception => {
        System.err.println("\nMAP: ERROR BUILDING DATA BASE\n")
        e.printStackTrace()
        System.exit(-1)}
		}
		
		ruleBase = Map[FuzzyRule, Array[ConsequentPart]]()
		inputValues = Array[Array[String]]()
		classLabels = Array[Byte]()
		
		/**
		 * Compute the cost of each class (currently only binary case)
		 */
		classCost = Array.fill(dataBase.getNumClasses())(1.0)
		if(Mediator.getCostSensitive()){
  		var numExamples: Array[Long] = dataBase.getClassNumExamples()
  		if (numExamples(0) < numExamples(1)){
  			classCost(1) = (numExamples(0).toDouble)/(numExamples(1).toDouble) // Maj = (1/IR)
  		}
  		else if (numExamples(0) > numExamples(1)){
  			classCost(0) = (numExamples(1).toDouble)/(numExamples(0).toDouble) // Maj = (1/IR)
  		}
		}
		logger.info("Setup Finished");
		this
	}
  
	/**
	 * Chi et al learning algorithm: 
	 * - One rule per example. 
	 * - Repeated rules are not taken into account (only consequents)
	 * - RWs are computed after all rules have been discovered
	 * - Double consequent rules are merged: class label is related with the highest RW
	 */
  def ruleBasePartition(index: Int, values: Iterator[String], sc: SparkContext): Iterator[KnowledgeBase] = {
    
    var logger = Logger.getLogger(this.getClass())

    var populationSet = Set[FuzzyRule]() //for EFS rule selection
    var kb = new KnowledgeBase(dataBase)  //final KB
    
    //Auxiliar vars
    var classEntry: Array[ConsequentPart] = null // (Index, RuleWeight) - for hash map during learning
    var input: Array[String] = null
    
    //Counters
    counter_class = Array.fill(dataBase.getNumClasses())(0) //Counter of occurrences of classes
    counter_rules = Array.fill(dataBase.getNumClasses())(0) //Counter of occurrences of rules
    startMs = System.currentTimeMillis()
    
    while(values.hasNext){ //for all input examples in given Map
      val value = values.next
      
      input = value.replaceAll(" ", "").split(",")
      
      if(input.length == (dataBase.getPosClassLabels() + 1)){
        val classIndex: Byte = dataBase.getClassIndex(input(dataBase.getPosClassLabels()))
        
        if (classIndex == -1){
  				throw new SecurityException("\nERROR RULES GENERATION: The class is not correct, this algorithm is not prepared for empty or N/A data, error="+input(dataBase.getPosClassLabels())+"*\n Check header file!")
  			}else{
          inputValues = inputValues :+ input      
          classLabels = classLabels :+ classIndex
          
          counter_class(classIndex) = counter_class(classIndex) + 1 
          
          val antecedents = dataBase.getRuleFromExample(input)
          val newRule = new FuzzyRule(antecedents, dataBase.getNumClasses())

          var consequent = Array[ConsequentPart]()
        	consequent = consequent :+ new ConsequentPart(classIndex, 1.0) //no initial weight
        	val aux = ruleBase.get(newRule)
        	if( aux == None){ //rule antecedents not generated yet
        	  ruleBase += (newRule -> consequent)
        	}else{
        	  classEntry = aux.get
        	  var contains: Boolean = false
        	  for(i <- 0 to (classEntry.length - 1)){
        	    if(classEntry(i).getClassIndex() == classIndex){ 
        	      contains = true       	   
        	    }
        	  }
            if (!contains){ //new class ("double" consequent rule)
            	classEntry = classEntry :+ (consequent(0))
            	ruleBase += (newRule -> classEntry)
            }
        	} 	
  			}
      }
    }//rules created
    
    //Uncomment for Debug
    /*
    println("Initial RB:")
    for (rule <- ruleBase){
      print("@ Index="+ index.toString+", Rule - " + rule._1.getAntecedent().deep.mkString(" "))
      for (consequents <- rule._2)
         print(" | C=" + consequents.getClassIndex() + " | W=" + consequents.getRuleWeight())
      println()
    }
    */
    
    /*
    var first: String = new String()
    var last: String = new String()
    first = first + "@ "+index+" First: ";
    for (input <- inputValues(0))
      first = first + input+", ";
    last = last + "@ "+index+" Last: ";
    for (input <- inputValues(inputValues.length-1))
      last = last + input+", ";
    println(first);
    println(last);
    */
    
    //if no rule weights selected this is unnecessary
    //println("Mira: "+rw+" / "+KnowledgeBase.RW_NONE);
    if (rw != KnowledgeBase.RW_NONE)
      computeMatchingDegreesAll();
         
    var j = 0
    for (rule <- ruleBase){
      var consequent : ConsequentPart = null;
      if (rw == KnowledgeBase.RW_NONE){
        consequent = rule._2(0);
      }
      else{
        consequent = computeConsequent(rule._2, j);
      }
      //logger.info("@Index"+index+" Rule - " + rule._1.getAntecedent().deep.mkString(" | ") + " | C=" + consequent.getClassIndex() + " | W=" + consequent.getRuleWeight())
      if(consequent.getRuleWeight() > 0){
        val res = new FuzzyRule(rule._1.getAntecedent(), consequent.getClassIndex(), consequent.getRuleWeight(), dataBase.getNumClasses())
        //logger.info("@ Index="+ index.toString+", Rule - " + res.getAntecedent().deep.mkString(" ") + " | C=" + res.getClassIndex() + " | W=" + res.getRuleWeight())
        counter_rules(consequent.getClassIndex()) = counter_rules(consequent.getClassIndex()) + 1 

        kb.addFuzzyRule(res)
      }
      j += 1
    }
    
    kb.initCounterRules(counter_rules)
    
    //EFS part: Rule Selection. NumEvaluations must be > 0
    var pop = new Population()
		if (kb.size() > 0 && numEvaluations > 0){
		  //uncomment for debug
		  //println("kb before=" + kb.toString())
			pop = new Population(kb,popSize,numEvaluations,1.0,62,alpha,inputValues,classLabels)
			kb = pop.Generation(sc)
		}

    //uncomment for debug
    //println("@ Map= "+index.toString+" | Counter class=> Positive= " + kb.getCounter().getPositive() + ", Negative= " + kb.getCounter().getNegative())
     
    writeExecutionTime(sc, index);
		
		//kb.addCounterClass(counter_class)
		logger.info("Finishing rule generation");
		Iterator(kb)
  }
  
  def writeExecutionTime(sc: SparkContext, index: Int){
     /**
		 *  Write execution time
		 */    
		endMs = System.currentTimeMillis()
		try {
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
  		var textPath: Path = null
		  if(!fs.exists(new Path(time_outputFile+"//mapper"+index+".txt"))){
			  textPath = new Path(time_outputFile+"//mapper"+index+".txt")
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(time_outputFile+"//mapper"+index+"_"+aux+i+".txt"))){
    			  textPath = new Path(time_outputFile+"//mapper"+index+"_"+aux+i+".txt")
    			  create = true
    		  }
		      i = i + 1
		    }
		  }
		  
		  var bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath,true)))
		  
    	bwText.write("Execution time (seconds): "+((endMs-startMs)/1000.0))
    	bwText.close()
      
		}catch{
		  case e: Exception => {
	    System.err.println("\nMAPPER: ERROR WRITING EXECUTION TIME")
		  e.printStackTrace()
		  System.err.println(-1)
		 }
		}
  }
  
}

/**
 * Distributed RulesGenerationCS class.
 *
 * @author Eva M. Almansa 
 */
object RulesGenerationCS {
  /**
   * Initial setting necessary.
   */
  def setup(sc: SparkContext, conf: SparkConf) = {
    new RulesGenerationCS().setup(sc, conf)
  }
}
