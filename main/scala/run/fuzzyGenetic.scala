package run

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import classifier.{ClassifierLauncher, ConfusionMatrixMapper, ConfusionMatrixReducer}
import core.{DataBase, Mediator, FuzzyRule, KnowledgeBase, NominalVariable, Population}
import learner.{LearnerLauncher, RulesGenerationCS, RulesGenerationReducer}
import utils.Randomize

/**
 * fuzzyGenetic: the main program for the Chi-Spark-CS-RS algorithm
 * 
 * @author Eva Almansa (eva.m.almansa@gmail.com)
 * @author Alberto Fernandez (alberto@decsai.ugr.es) - University of Granada
 * @version 1.0 (E. Almansa) - 05-feb-2017
 * @version 1.1 (A. Fernandez) - 12-jun-2017
 * 
 * It contains a Spark distributed implementation for the Chi FRBCS learning algorithm. It incorporates both cost-sensitive learning
 * and evolutionary rule selection (both are optional). 
 */
object fuzzyGenetic {

	private val EXECUTIONS = Array(16,32,64,128);
	private val NUM_PROGRAM_ARGS: Int = 7;
	private val NUM_PARAMS: Int = 9;
	private val COMMAND_STR: String = ("\n  <Spark_path>spark-submit --master <local[*] or spark://hadoop-master:7077> --class run.fuzzyGenetic <jar_file>" 
			+ "\n  <parameters_file:" 
			+ "\n    {inference,number_of_labels,num_individuals,num_evaluations,cross_validation}>"
			+ "\n  <input_path(Folder)> <header_file>" 
			+ "\n  <name_inputFile> <TEST file: tra.dat or tst.dat> OR <inputTrainFile> <inputTestFile>" 
			+ "\n   (iteratively inside ->" 
			+ "\n         IF cross_validation > 0" 
			+ "\n         THEN <name_inputFile> + -5-?tra.dat AND <TEST file: tra.dat or tst.dat>"
			+ "\n         ELSE <inputTrainFile> <inputTestFile>)" 
			+ "\n  <number_of_partitions> <ouput_path(Folder)>\n");

	private var jobName = "Chi-Spark-CS-RS_";

	/**
	 * Variables 
	 */
	private var sc: SparkContext = null;
	private var startMs, endMs: Long = 0;
	private var logger: Logger = null; 

	/**
	 * Variables of the Fuzzy-Chi-CS 
	 */
	private var kb: KnowledgeBase = null;

	/**
	 * Main function. It reads the parameters and launch the learning algorithm and the classification
	 * 
	 * Two possible options: CV (iterated X times) and Standard (1 call). 
	 * 
	 * Learning stage is carried out within "learnerLauncher" method
	 * Classification stage (train and test) is carried out within "classifierlLauncher" method
	 */
	def main(args: Array[String]) {

		logger = Logger.getLogger(this.getClass());

		if (args.length != NUM_PROGRAM_ARGS) {
			logger.error("=> wrong parameters number")
			System.err.println(" Usage: " + COMMAND_STR)
			System.exit(1)
		}

		//uncomment the following line in case a scalability study is required
		//for (maps <- EXECUTIONS){
		/**
		 * SPARK CONFIGURATION
		 */
		jobName = jobName + args(5);
		val conf = new SparkConf().setAppName(jobName); //to use local Master -Dspark.master=local[*] in VM parameters
		sc = new SparkContext(conf);
		Mediator.setConfiguration(sc.getConf);

		/**
		 * READING PARAMETERS
		 */
		val paramsFile = args(0);  //configuration of the FRBCS
		val inputPath = args(1)+"/"; //folder with the input files
		val headerFile = args(2); //name of the header file with information about the dataset
		val inputFile = args(3); //training set. In case cross-validation is set in config file, use dataset name for training instead of filename
		val inputTestFile = args(4); //test set. In case cross-validation is set in config file, use suffix for test, i.e. tst
		val nPartitions = args(5).toInt; //nPartitions == number of Maps

		/**
		 * STORE ALGORITHM PARAMETERS AND READ HEADER FILE
		 */
		logger.info("Storing Chi and Selection parameters...");
		var CS_RS: Array[Boolean] = null;
		try{
			CS_RS = Mediator.storeChiParameters(NUM_PARAMS, paramsFile); 			//params are not set, but only stored in the configuration
		}
		catch {
		case e: Exception => {
			logger.error("ERROR READING PARAMETERS");
			System.err.println("ERROR READING PARAMETERS:\n");
			e.printStackTrace();
			System.exit(-1);
		}
		}

		//The output folder is renamed depending on whether cost-sensitive learning and rule selection are used
		var name_CS, name_RS = "";
		if(CS_RS(0))
			name_CS = "_CS";
		if(CS_RS(1))
			name_RS = "_RS";
		val outputPath = args(6) + name_CS + name_RS + "_" + nPartitions;

		//Some information regarding the configuration parameters is given
		logger.info("=> jobName \"" + jobName + "\"")
		logger.info("=> Params File (Number of Labels and Option) \"" + paramsFile + "\"")
		logger.info("=> Input Path (Folder) \"" + inputPath + "\"")
		logger.info("=> Header File \"" + headerFile + "\"")
		logger.info("=> Input File \"" + inputFile + "\"")
		logger.info("=> Input Test File \"" + inputTestFile + "\"")
		logger.info("=> Number of Partitions \"" + nPartitions + "\"")
		logger.info("=> Ouput Path (Folder) \"" + outputPath + "\"")

		/**
		 * SAVE BASIC LEARNER PARAMETERS. It is shared by all tasks
		 */  
		logger.info("Save basic parameters...")
		Mediator.saveLearnerInputPath(inputPath)
		Mediator.saveHeaderPath(inputPath+headerFile)
		try{
			Mediator.saveLearnerOutputPath(sc, outputPath)
		}catch{
		case e: Exception => {
			logger.error("ERROR IN LEARNER OUTPUT PATH");
			System.err.println("ERROR IN LEARNER OUTPUT PATH:\n")
			e.printStackTrace()
			System.exit(-1)
		}      
		}

		/**
		 * SAVE BASIC CLASSIFIER PARAMETERS. This information is shared by all tasks (independent JVMs)
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
			Mediator.readConfiguration(Mediator.getConfiguration()); //parameters are read from the configuration
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
		 * Standard learning (no automatic run of CV is selected 
		 */
		if (!Mediator.getOptionCrossValidation()){

			/**
			 * Learn the model
			 */
			learnerLauncher(inputPath + inputFile, nPartitions)

			/**
			 * Classify Test
			 */
			//classifierLauncher(false, inputTestFile, nPartitions, true)

			/**
			 * Classify Train
			 */
			//classifierLauncher(true, inputFile, nPartitions, true)

		}else{
			/**
			 * Cross Validation, for loop
			 */
			var last_iteration = false;
			for (iteration <- 1 to Mediator.getCrossValidation()){

				/**
				 * Learner
				 */
				learnerLauncher(inputPath + inputFile + "-5-" + iteration.toString + "tra.dat", nPartitions)


				if(iteration == Mediator.getCrossValidation())
					last_iteration = true;

				/**
				 * Classifier
				 */
				classifierLauncher(false, inputFile + "-5-" + iteration.toString + inputTestFile, nPartitions, last_iteration);
				classifierLauncher(true, inputFile + "-5-" + iteration.toString + "tra.dat", nPartitions, last_iteration);
			}
			//} //uncomment this line for running different number of Maps (change maps variable for nPartitions)

		}
		sc.stop(); //end of the program
	}

	/**
	 * Method for the learning stage. It requires the input training file and the number of Maps (partitions)
	 * 
	 * First, the setup method initializes all the necessary data structures for the learning
	 * 
	 * Then, different "map" tasks (depending on the selected partitions) are executed in a distributive way, each associated
	 * with a chunk of the original data.
	 * 
	 * A single KB is obtained from within each Map. Then, the Reduce stage takes the KBs by pairs and aggregates them: rules with the
	 * same antecedent are merged into one. In case of same consequents the average RW is stored. For different consequents, the one with the 
	 * highest RW is kept. 
	 * 
	 * An evolutionary rule selection process can be run within each Map task (nEvaluations > 0), just after the rules have been discovered  
	 */
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
			 * Rules Generation (A Cost Sensitive approach may be used for the RWs)
			 */
			val rulesGenerationCS = RulesGenerationCS.setup(sc, Mediator.getConfiguration()); //initialize data structures

			//Be careful: training file is sequentially partitioned, shuffle class examples is highly recommended
			var output = sc.textFile(inputFile, nPartitions) 
					.mapPartitionsWithIndex((index, trainPart) => rulesGenerationCS.ruleBasePartition(index, trainPart, sc))
					.cache();

			kb = output.reduce((op1, op2) => RulesGenerationReducer.reduce(op1, op2));
			kb.updateRules(); 
			endMs = System.currentTimeMillis();

			//uncomment for debug
			/*
			println("@ RB size="+ kb.getSizeRuleBase());
			println("@ Final Rule Base="+kb.counterClassLabels().deep.mkString(" "));
			logger.info("Solution Rule Base...")
			for(rule <- kb.getRuleBase()){
          println("@ Final Rule Base= "+rule.getAntecedent().mkString(" ") + " | C=" + rule.getClassIndex() + ", Weight=" + rule.getRuleWeight() + ", Weight Counter=" + rule.getRuleWeightCounter().deep.mkString(" "))
        }
       */
		}catch {
		case e: Exception => {
			logger.error("ERROR LAUNCHING MAPREDUCE:\n")
			logger.error(e.toString())
			System.exit(-1)
		}
		}

		/**
		 * SAVE RULEBASE
		 */
		try {
			logger.info("Saving Rule Base");
			LearnerLauncher.writeFinalRuleBase(kb, sc)
		} catch {
		case e: Exception => {
			logger.error("\nERROR MERGING STAGE 2 OUTPUT FILES")
			logger.error(e.toString)
			System.exit(-1)
		}
		}

		/**
		 * SAVE DATABASE
		 */
		try {
			logger.info("Saving Data Base");
			LearnerLauncher.writeDataBase(kb, sc) 
			kb.eraseCounterRules()
		}
		catch{
		case e: Exception => {
			logger.error("\nERROR WRITING DATA BASE\n")
			logger.error(e.toString) 
			System.exit(-1)
		}
		}

		/**
		 * WRITE EXECUTION TIME
		 */
		LearnerLauncher.writeExecutionTime(startMs, endMs, sc) 

		logger.info("@ ... END LEARNER LAUNCHER")

	}

	/**
	 * Classification method
	 * @param train boolean variable to determine whether the train or test set is evaluated (for storing final info values)
	 * @param inputFile the name of the file with the data (training or test)
	 * @param nPartitions number of Maps (partitions of the input data file)
	 * @param lastIteration boolean variable to determine whether the last iteration (in CV) is considering (for storing final info values)
	 */
	def classifierLauncher(train: Boolean, inputFile: String, nPartitions: Int, last_iteration: Boolean) {

		logger.info("@ START CLASSIFIER LAUNCHER...")

		startMs = System.currentTimeMillis()

		var AUC: Double = 0.0

		/**
		 * Confusion Matrix Accumulator
		 */
		def broadcastRuleBase(ruleBase: KnowledgeBase, sc: SparkContext) = sc.broadcast(ruleBase)   
		val confusionMatrixAccum = sc.accumulator(ConfusionMatrixReducer.zero(
				Array.fill(kb.getDataBase().getNumClasses(), kb.getDataBase.getNumClasses())(0)))(ConfusionMatrixReducer)

		/**
		 * Confusion Matrix
		 */
		try{

			val kbBC = broadcastRuleBase(kb, sc); //broadcast variable to share among tasks

			val confusionMatrixMapper = ConfusionMatrixMapper.setup(Mediator.getConfiguration());
			var confusionMatrix = sc.textFile(Mediator.getClassifierInputPath()+inputFile, nPartitions)
					.mapPartitions(testPart => confusionMatrixMapper.mapPartition(testPart, kbBC))
					.cache;

			println(confusionMatrix.count());       
			confusionMatrix.foreach ( cm => confusionMatrixAccum += cm );   //aggregate intermediatte values       

			kbBC.unpersist();
			kbBC.destroy();

			endMs = System.currentTimeMillis();

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