package org.apache.spark.run

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import classifier.{ClassifierLauncher, ConfusionMatrixMapper, ConfusionMatrixReducer}
import core.{DataBase, Mediator, FuzzyRule, KnowledgeBase, NominalVariable, Population}
import learner.{LearnerLauncher, RulesGenerationCS, RulesGenerationReducer}
import utils.Randomize

object fuzzyGenetic {
  
  private val EXECUTIONS = Array(16,32,64,128)
  private val NUM_PROGRAM_ARGS: Int = 7
  private val NUM_PARAMS: Int = 8
	private val COMMAND_STR: String = ("\n  <Spark_path>spark-submit --master <local[*] or spark://hadoop-master:7077> --class <name_class> <jar_file>" 
	                                   + "\n  <parameters_file:" 
	                                           + "\n    {inference,number_of_labels,num_individuals,num_evaluations,cross_validation}>"
	                                   + "\n  <input_path(Folder)> <header_file>" 
	                                   + "\n  <name_inputFile> <TEST file: tra.dat or tst.dat> OR <inputTrainFile> <inputTestFile>" 
	                                           + "\n   (iteratively inside ->" 
	                                           + "\n         IF cross_validation > 0" 
	                                           + "\n         THEN <name_inputFile> + -5-?tra.dat AND <TEST file: tra.dat or tst.dat>"
	                                           + "\n         ELSE <inputTrainFile> <inputTestFile>)" 
	                                   + "\n  <number_of_partitions> <ouput_path(Folder)>\n")
	
	private var jobName = "Chi-Spark-SC-SR_"
	
	/**
	 * Variables 
	 */
	private var sc: SparkContext = null
	private var startMs, endMs: Long = 0
  private var logger: Logger = null 
  
  /**
   * Variables of the Fuzzy-Chi-CS 
   */
  private var kb: KnowledgeBase = null
  
  def main(args: Array[String]) {
    
    logger = Logger.getLogger(this.getClass())
    
    if (args.length != NUM_PROGRAM_ARGS) {
      logger.error("=> wrong parameters number")
      System.err.println(" Usage: " + COMMAND_STR)
      System.exit(1)
    }
    
    //for (maps <- EXECUTIONS){
      /**
  		 * SPARK CONFIGURATION
  		 */
      jobName = jobName + args(5) 
      val conf = new SparkConf().setAppName(jobName)
      sc = new SparkContext(conf)
      Mediator.setConfiguration(sc.getConf)
      
      /**
  		 * READING PARAMETERS
  		 */
      val paramsFile = args(0)
      val inputPath = args(1)
      val headerFile = args(2)
      val inputFile = args(3)
      val inputTestFile = args(4)
      val nPartitions = args(5).toInt //maps//
      
      /**
  		 * STORE ALGORITHM PARAMETERS AND READ HEADER FILE
  		 */
  		logger.info("Store Chi and Selection parameters...")
  		var CS_RS: Array[Boolean] = null
  		try{
  			CS_RS = Mediator.storeChiParameters(NUM_PARAMS, paramsFile) 			
  		}
  		catch {
  		   case e: Exception => {
  		       System.err.println("ERROR READING PARAMETERS:\n")
  		       e.printStackTrace()
  		       System.exit(-1)
  		     }
  		}
  		
  		var name_CS, name_RS = ""
      if(CS_RS(0))
        name_CS = "_CS"
      if(CS_RS(1))
        name_RS = "_RS"
  		val outputPath = args(6) + name_CS + name_RS + "_" + nPartitions
  		
  		logger.info("=> jobName \"" + jobName + "\"")
      //logger.info("=> HDFS Path \"" + hdfsLocation + "\"")
  		logger.info("=> Params File (Number of Labels and Option) \"" + paramsFile + "\"")
      logger.info("=> Input Path (Folder) \"" + inputPath + "\"")
      logger.info("=> Header File \"" + headerFile + "\"")
      logger.info("=> Input File \"" + inputFile + "\"")
      logger.info("=> Input Test File \"" + inputTestFile + "\"")
      logger.info("=> Number of Partitions \"" + nPartitions + "\"")
      logger.info("=> Ouput Path (Folder) \"" + outputPath + "\"")
      
  		/**
  		 * SAVE BASIC LEARNER PARAMETERS 
  		 */  
      logger.info("Save basic parameters...")
  		Mediator.saveLearnerInputPath(inputPath)
  		Mediator.saveHeaderPath(inputPath+headerFile)
  		try{
    		Mediator.saveLearnerOutputPath(sc, outputPath)
      }catch{
        case e: Exception => {
          System.err.println("ERROR IN LEARNER OUPUT PATH:\n")
          e.printStackTrace()
          System.exit(-1)
        }      
      }
      
      /**
  		 * SAVE BASIC CLASSIFIER PARAMETERS
  		 */
  		Mediator.saveClassifierInputPath(inputPath)
  		Mediator.saveHeaderPath(inputPath+headerFile)
  		Mediator.saveClassifierOutputPath(outputPath)
  		Mediator.saveClassifierRuleBasePath(outputPath+"//RB")
  		Mediator.saveClassifierDataBasePath(outputPath+"//DB")
      
      /**
  		 * READ CONFIGURATION
  		 */ 
  		try{
  			Mediator.readConfiguration(Mediator.getConfiguration())
  		}
  		catch{
  		   case e: Exception => 
  		     {
  		       System.err.println("ERROR READING CONFIGURATION:\n")
  		       e.printStackTrace()
  		       System.exit(1)
  		     }
  		}
  			
  		/**
    	 * One Iteration
    	 */
  		if (!Mediator.getOptionCrossValidation()){
  		  /**
        	 * Learner
        	 */
          learnerLauncher(inputPath + inputFile, nPartitions)
          
          /**
        	 * Classifier Test
        	 */
          classifierLauncher(false, inputTestFile, nPartitions, true)
          
          /**
        	 * Classifier Train
        	 */
          classifierLauncher(true, inputFile, nPartitions, true)
  		}else{
        /**
      	 * Cross Validation
      	 */
        var last_iteration = false
        for (iteration <- 1 to Mediator.getCrossValidation()){
          
          /**
        	 * Learner
        	 */
          learnerLauncher(inputPath + inputFile + "-5-" + iteration.toString + "tra.dat", nPartitions)
          
          /**
        	 * Classifier
        	 */
          if(iteration == Mediator.getCrossValidation())
            last_iteration = true
          classifierLauncher(false, inputFile + "-5-" + iteration.toString + inputTestFile, nPartitions, last_iteration)
          
          classifierLauncher(true, inputFile + "-5-" + iteration.toString + "tra.dat", nPartitions, last_iteration)
        }
  		//}
  		
      sc.stop()
    }
    
    def learnerLauncher(inputFile: String, nPartitions: Int){
      
      logger.info("@ START LEARNER LAUNCHER...")
                        
      startMs = System.currentTimeMillis()
      
  		/**
  		 * LEARNER PROCESS
  		 */
      try{    
     
        /**
    		 * Initial Seed only necessary in Genetic Algorithm
    		 */
        Randomize.setSeed(Mediator.getInitSeed)
        
        /**
    		 * Rules Generate CS
    		 */
        val rulesGenerationCS = RulesGenerationCS.setup(sc, Mediator.getConfiguration())
        
        var output = sc.textFile(inputFile, nPartitions) 
                        .mapPartitionsWithIndex((index, trainPart) => rulesGenerationCS.ruleBasePartition(index, trainPart, sc))
                        .cache()
         
         
         kb = output.reduce((op1, op2) => RulesGenerationReducer.reduce(op1, op2))
            
        /*if (kb.size() > 0 && Mediator.getNumEvaluations() > 0){
    		  var pop = new Population()
    		  logger.info("@ START POPULATION LAUNCHER...")
    		  
    		  var inputValues = Array[Array[String]]()
    		  var classLabels = Array[Byte]()
    		  var classIndex: Byte = 0
          
    		  var values = sc.textFile(inputFile, 1).collect() 
    		  for(value <- values){
            var input: Array[String] = null
            input = value.replaceAll(" ", "").split(",")
            if(!value.isEmpty){
              inputValues = inputValues :+ input
              classIndex = kb.getDataBase().getClassIndex(input(kb.getDataBase().getPosClassLabels()))
              classLabels = classLabels :+ classIndex
            }
          }
    		  pop = new Population(kb,Mediator.getPopSize(),Mediator.getNumEvaluations(),1.0,62,inputValues,classLabels)
    		  
    		  logger.info("@ START GENERATION...")
    		  
    		  kb = pop.Generation_Global(sc, nPartitions)
    		  
    		  logger.info("@ ... END POPULATION LAUNCHER")
    		}*/
        endMs = System.currentTimeMillis()
        
        println("@ RB size="+ kb.getSizeRuleBase())
        println("@ Final Rule Base="+kb.counterClassLabels().deep.mkString(" "))
        //logger.info("Solution Rule Base...")
        /*for(rule <- kb.getRuleBase()){
          println("@ Final Rule Base= "+rule.getAntecedent().mkString(" ") + " | C=" + rule.getClassIndex() + ", Weight=" + rule.getRuleWeight() + ", Weight Counter=" + rule.getRuleWeightCounter().deep.mkString(" "))
        }*/
      }catch {
    	  case e: Exception => {
    	    System.err.println("ERROR LAUNCHING MAPREDUCE:\n")
    		  e.printStackTrace()
    		  System.exit(-1)
    	  }
  	  }
      
      /**
  		 * SAVE RULEBASE
  		 */
  		try {
  			LearnerLauncher.writeFinalRuleBase(kb, sc)
  		} catch {
  		  case e: Exception => {
  		    println("\nERROR MERGING STAGE 2 OUTPUT FILES")
  			  e.printStackTrace()
  			  System.exit(-1)
  		  }
  		}
  
  		/**
  		 * SAVE DATABASE
  		 */
  		try {
  			LearnerLauncher.writeDataBase(kb, sc) // Save data base
  			kb.eraseCounterRules()
  			//kb.eraseCounterClass()
  		}
  		catch{
  		  case e: Exception => {
  		   println("\nERROR WRITING DATA BASE\n")
  			 e.printStackTrace() 
  			 System.exit(-1)
  		  }
  		}
  
  		/**
  		 * WRITE EXECUTION TIME
  		 */
  		LearnerLauncher.writeExecutionTime(startMs, endMs, sc) 
  
  		logger.info("@ ... END LEARNER LAUNCHER")
  		
    }
  }
  
  /**
	 * Classifier method
	 * @author Eva M. Almansa
	 */
	def classifierLauncher(train: Boolean, inputFile: String, nPartitions: Int, last_iteration: Boolean) {
	 
	  logger.info("@ START CLASSIFIER LAUNCHER...")
	  
	  startMs = System.currentTimeMillis()
	  
	  var AUC: Double = 0.0
      
		/**
		 * Confusion Matrix Accumulator
		 */
    def broadcastRuleBase(ruleBase: KnowledgeBase, sc: SparkContext) = sc.broadcast(ruleBase)   
    val confusionMatrixAccum = sc.accumulator(ConfusionMatrixReducer.zero(Array.fill(kb.getDataBase().getNumClasses(), kb.getDataBase.getNumClasses())(0)))(ConfusionMatrixReducer)
      
	  /**
		 * Confusion Matrix
		 */
		try{
  
      val kbBC = broadcastRuleBase(kb, sc)
      
      val confusionMatrixMapper = ConfusionMatrixMapper.setup(Mediator.getConfiguration())
      var confusionMatrix = sc.textFile(Mediator.getClassifierInputPath()+inputFile, nPartitions)
                              .mapPartitions(testPart => confusionMatrixMapper.mapPartition(testPart, kbBC))
                              .cache
      
      println(confusionMatrix.count())       
      confusionMatrix.foreach ( cm => confusionMatrixAccum += cm )          
      
      kbBC.unpersist()
      kbBC.destroy()
      
      endMs = System.currentTimeMillis()
      
      //logger.info(" Confusion Matrix => ")       
      AUC = ConfusionMatrixReducer.metricsConfusionMatrix(confusionMatrixAccum.value, kb.getDataBase)
        
		}
		catch{
		  case e: Exception => {
			  println("\nERROR RUNNING MAPPARTITION:\n")
			  e.printStackTrace()
			  System.exit(-1)
		  }
		}
		
		/**
		 * WRITE EXECUTION TIME
		 */
		try {
			ClassifierLauncher.writeExecutionTime(train, startMs, endMs, sc)
		} catch {
		  case e: Exception => {
			  println("\nERROR EXECUTION TIME\n")
			  e.printStackTrace()
			  System.exit(-1)
		  }
		}

		/**
		 * WRITE CONFUSSION MATRIX AND AUC
		 */
		try {
			var output: String = ClassifierLauncher.writeConfusionMatrix(train, confusionMatrixAccum.value, kb.getDataBase, sc)
			ClassifierLauncher.write_avgAUC_MG(train, AUC, last_iteration, sc)
			//logger.info(output)
		} catch {
		  case e: Exception => {
			  println("\nERROR WRITING CONFUSSION MATRIX\n")
			  e.printStackTrace()
			  System.exit(-1)
		  }
		}

		logger.info("@ ... END CLASSIFIER LAUNCHER")
	}
}