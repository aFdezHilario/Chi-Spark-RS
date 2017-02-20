package core

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.io.Source

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.StringTokenizer

/**
 * It contains the shared constant variables which define the data management and
 * set the information related to the structure of the database from the header file.
 *
 * @author Eva M. Almansa
 * @version 1.0
 */
object Mediator {
  /**
	 * Class labels field
	 */
	private val CLASS_LABELS_FIELD = "class_labels"

	/**
	 * Classifier database path field
	 */
	private val CLASSIFIER_DATABASE_PATH_FIELD = "classifier_database_path"

	/**
	 * Classifier input path field
	 */
	private val CLASSIFIER_INPUT_PATH_FIELD = "classifier_input_path"

	/**
	 * Classifier output path field
	 */
	private val CLASSIFIER_OUTPUT_PATH_FIELD = "classifier_output_path"

	/**
	 * Classifier rule base path field
	 */
	private val CLASSIFIER_RULE_BASE_PATH_FIELD = "classifier_rule_base_path"

	/**
	 * Cost-sensitive field
	 */
	private val COST_SENSITIVE_FIELD = "cost_sensitive"
	
	/**
	 * Dataset header path
	 */
	private val DATASET_HEADER_PATH = "dataset_header_path"

	/**
	 * Fuzzy Reasoning Method field
	 */
	private val FRM_FIELD = "inference"
	
	/**
	 * Initial seed (only necessary in Genetic Process)
	 */
	private val INIT_SEED = "init_seed"

	/**
	 * Learner training input path field
	 */
	private val LEARNER_INPUT_PATH_FIELD = "learner_input_path"
	
	/**
	 * Learner output path field
	 */
	private val LEARNER_OUTPUT_PATH_FIELD = "learner_output_path"

	/**
	 * Learner rule base (rule base that is learning) size field
	 */
	private val LEARNER_RULE_BASE_SIZE_FIELD = "learner_rule_base_size"

	/**
	 * Number of linguistic labels field
	 */
	private val NUM_LINGUISTIC_LABELS_FIELD = "num_linguistic_labels"

	/**
	 * Number of iteration on Cross Validation
	 */
	private val CROSS_VALIDATION = "cross_validation"
	
	/**
	 * PATH FOR TIME STATS
	 */
	final val TIME_STATS_DIR = "time"
	
	/**
	 * Variables's Genetic Algorithm: CHC
	 */
	private val EVALUATIONS_FIELD = "num_evaluations"
	
	private val POPULATION_FIELD = "num_individuals"
	
	private val ALPHA = "alpha"
	
	/**
	 * Number of examples of each class
	 */
	private var classNumExamples: Array[Long] = null

	/**
	 * Classifier database path
	 */
	private var classifierDatabasePath: String = null 

	/**
	 * Classifier input directory path
	 */
	private var classifierInputPath: String = null

	/**
	 * Classifier output directory path
	 */
	private var classifierOutputPath: String = null

	/**
	 * Classifier rule base path
	 */
	private var classifierRuleBasePath: String = null

	/**
	 * Configuration object
	 */
	private var configuration: SparkConf = null

	/**
	 * True for cost-sensitive rule weight computation (only for binary datasets)
	 */
	private var costSensitive: Boolean = false

	/**
	 * Fuzzy Reasoning Method
	 */
	private var frm: Byte = 0

	/**
	 * Dataset Header Path
	 */
	private var headerPath: String = null
	
	/**
	 * Learner input directory path
	 */
	private var learnerInputPath: String = null

	/**
	 * Learner output directory path
	 */
	private var learnerOutputPath: String = null

	/**
	 * Learner rule base (rule base that is learning) size
	 */
	private var learnerRuleBaseSize: Byte = 0

	/**
	 * Number of linguistic labels (fuzzy sets) considered for all fuzzy variables
	 */
	private var numLinguisticLabels: Byte = 0
	
	/**
	 * Number of iteration on Cross Validation
	 */
	private var crossValidation: Byte = 0
	
	/**
	 * If crossValidation parameter is equal or lesser than 0, will be false otherwise is true 
	 */
	private var optionCrossValidation: Boolean = false
  
  /**
	 * Variables's Genetic Algorithm
	 */
  private var populationSize: Int = 0
	
	private var numEvaluations: Int = 0
	
	private var alpha: Double = 0.0
  
	private var init_seed: Long = 0
	
  /**
  * Create a RB output folder 
  */
  def createRBfolder(sc: SparkContext){
    
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	
		//Create to Learner Output Path
		val conf = sc.hadoopConfiguration
		conf.set("RB", Mediator.getLearnerRuleBasePath())
		val fs = FileSystem.get(conf)
		if(!fs.exists(new Path(Mediator.getLearnerRuleBasePath())))
		  fs.mkdirs(new Path(Mediator.getLearnerRuleBasePath()))
  }
  
 /**
  * Create a DB output folder
  */
  def createDBfolder(sc: SparkContext){
    
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	
		//Create to Learner Output Path
		val conf = sc.hadoopConfiguration
		//conf.set("DB", Mediator.getLearnerDataBasePath())
		val fs = FileSystem.get(conf)
		if(!fs.exists(new Path(getLearnerDataBasePath())))
		  fs.mkdirs(new Path(getLearnerDataBasePath()))
  }
  
 /**
  * Create a Time output folder
  */
  def createTimefolder(sc: SparkContext){
    
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	
		//Create to Learner Output Path
		val conf = sc.hadoopConfiguration
		val fs = FileSystem.get(conf)
		if(!fs.exists(new Path(getLearnerOutputPath()+"//"+TIME_STATS_DIR)))
		  fs.mkdirs(new Path(getLearnerOutputPath()+"//"+TIME_STATS_DIR))
  }
	
  /**
  * Create a information output folder
  */
  def createInformationfolder(sc: SparkContext){
    
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	
		//Create to Learner Output Path
		val conf = sc.hadoopConfiguration
		//conf.set("DB", Mediator.getLearnerDataBasePath())
		val fs = FileSystem.get(conf)
		if(!fs.exists(new Path(getLearnerDataBasePath())))
		  fs.mkdirs(new Path(getLearnerOutputPath()+"//Information"))
  }
  
  /**
	 * Returns alpha for the genetic algorithm
	 * @return alpha for the genetic algorithm
	 */
	def getAlpha(): Double = alpha
	
	/**
	 * Returns classifier database path
	 * @return classifier database path
	 */
	def getClassifierDatabasePath (): String = classifierDatabasePath

	/**
	 * Returns classifier input path
	 * @return classifier input path
	 */
	def getClassifierInputPath (): String =	classifierInputPath

	/**
	 * Returns classifier output path
	 * @return classifier output path
	 */
	def getClassifierOutputPath (): String = classifierOutputPath

	/**
	 * Returns classifier rule base path
	 * @return classifier rule base path
	 */
	def getClassifierRuleBasePath (): String = classifierRuleBasePath

	/**
	 * Returns classifier temporary output path
	 * @return classifier temporary output path
	 */
	def getClassifierTmpOutputPath (): String = classifierOutputPath+"_TMP"
  
	/**
	 * Returns the configuration object
	 * @return Configuration object
	 */
	def getConfiguration (): SparkConf = configuration

	/**
	 * Returns if cost sensitive function is allowed
	 * @return true: if cost sensitive function is allowed
	 */
  def getCostSensitive(): Boolean = costSensitive 
  
	/**
	 * Returns the size of poblation for the genetic algorithm
	 * @return size of poblation for the genetic algorithm
	 */
  def getCrossValidation(): Byte = crossValidation 
  
	/**
	 * Returns the Fuzzy Reasoning Method used for the inference
	 * @return 0 for Winning Rule and 1 for Additive Combination
	 */
	def getFRM (): Byte = frm
	
	/**
	 * Returns the dataset header path
	 * @return dataset header path
	 */
	def getHeaderPath(): String = headerPath+"/"

	/**
	 * Returns initial seed (only necessary in Genetic Process)
	 * @return initial seed
	 */
	def getInitSeed(): Long = init_seed 

	/**
   * Gets the labels of a feature or the main class as Array[String].
   * @param str string to parser
   * @return gets the labels of a feature or the main class
   */
  def getLabels (str: String): Array[String] = str.substring(str.indexOf("{") + 1, str.indexOf("}")).replaceAll(" ", "").split(",")
  
	/**
	 * Returns learner database path
	 * @learner database path
	 */
	def getLearnerDataBasePath (): String = learnerOutputPath+"//DB"

	/**
	 * Returns learner input path
	 * @learner input path
	 */
	def getLearnerInputPath (): String = learnerInputPath

	/**
	 * Returns learner output path
	 * @learner output path
	 */
	def getLearnerOutputPath (): String = learnerOutputPath

	/**
	 * Returns learner rule base path
	 * @learner rule base path
	 */
	def getLearnerRuleBasePath (): String = learnerOutputPath+"//RB"

	/**
	 * Returns learner rule base (rule base that is learning) size (number of rules)
	 * @return learner rule base size (number of rules)
	 */
	def getLearnerRuleBaseSize (): Byte = learnerRuleBaseSize
  
	/**
	 * Returns the number of linguistic labels (fuzzy sets) considered for all variables
	 * @return number of linguistic labels (fuzzy sets) considered for all variables
	 */
	def getNumLinguisticLabels (): Byte = numLinguisticLabels
	
	/**
	 * Returns the number of evaluation for the genetic algorithm
	 * @return number of evaluation for the genetic algorithm
	 */
	def getNumEvaluations(): Int = numEvaluations
  
	/**
	 * Returns true if the cross validation is permitted
	 * @return true if the cross validation is permitted
	 */
  def getOptionCrossValidation(): Boolean = optionCrossValidation
  
	/**
	 * Returns the size of poblation for the genetic algorithm
	 * @return size of poblation for the genetic algorithm
	 */
  def getPopSize(): Int = populationSize
  
  /**
   * Get the min and max of a feature as a Array[Double].
   * @param str string to parser
   */
  def getRange (str: String): Array[Double] = {
    var aux = str.substring(str.indexOf("[") + 1, str.indexOf("]")).replaceAll(" ", "").split(",")
    var result = new Array[Double](2)
    result(0) = aux(0).toDouble
    result(1) = aux(1).toDouble
    result
  }
	
	/**
	 * Reads classifier configuration (no variables and class labels are read here) from the specified configuration file
	 * @param conf Configuration instance where all the objects and parameters of this class are stored (in the future this configuration instance will be considered)
	 * @throws Base64DecodingException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	def readClassifierConfiguration (conf: SparkConf) /*throws Base64DecodingException, IOException, ClassNotFoundException*/{

		// Read basic configuration
	  headerPath = conf.get(DATASET_HEADER_PATH)
		classifierInputPath = conf.get(CLASSIFIER_INPUT_PATH_FIELD)
		classifierOutputPath = conf.get(CLASSIFIER_OUTPUT_PATH_FIELD)
		classifierDatabasePath = conf.get(CLASSIFIER_DATABASE_PATH_FIELD)
		classifierRuleBasePath = conf.get(CLASSIFIER_RULE_BASE_PATH_FIELD)

		var frmStr = conf.get(FRM_FIELD)
		if(frmStr.contentEquals("wr") || frmStr.contentEquals("winningrule") || frmStr.contentEquals("winning_rule")){
		  frm = KnowledgeBase.FRM_WINNING_RULE
		}
		else{
		  frm = KnowledgeBase.FRM_ADDITIVE_COMBINATION
		}
		
		numLinguisticLabels = conf.get(NUM_LINGUISTIC_LABELS_FIELD).toByte
		crossValidation = conf.get(CROSS_VALIDATION).toByte
		
		var buffer: String = ""

		configuration = conf

	}

	/**
	 * Reads the number of examples of each class from the header file
	 * @param filePath file path
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	def readClassNumExamplesFromHeaderFile (headerFile: String, sc: SparkContext) /*throws IOException, URISyntaxException*/{
	  
	  try{
      val lines = sc.textFile(headerFile, 1).collect()
          
      var buffer: String = null
    	var st: StringTokenizer = null
      
    	var output: String = ""
    	var variablesTmp = new ListBuffer[Variable]
    	var classNumExamples: Array[Long] = null
    
    	var num_linguistic_labels = Mediator.getNumLinguisticLabels()
      
    	for (line <- lines){
    
    		buffer = line.replaceAll(", ", ",")
    		st = new StringTokenizer (buffer)
    		var field = st.nextToken()
    		
    		if (field.contentEquals("@numInstancesByClass")){
    
    				st = new StringTokenizer (st.nextToken(),", ")
    				
    				classNumExamples = Array.fill(st.countTokens())(0.toLong)
    				var i = 0
    				while (st.hasMoreTokens()){
    					classNumExamples(i) = st.nextToken().toLong
    					i = i.+(1)
    				}
    		}
    	}
      
      if (classNumExamples == null){
				throw new IllegalArgumentException("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n")
			}
	  }catch {
  		  case e: Exception => {
          System.err.println("\nERROR READING HEADER FILE\n")
          e.printStackTrace()
          System.exit(-1)}
	  }

	}

	def readRuleBaseTmp(sc: SparkContext, dataBase: DataBase): Array[FuzzyRule] = {
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  
	  var ruleBase = Array[FuzzyRule]()
	  // Read the rule base
		try{	    	

			val ruleBaselines = sc.textFile(Mediator.getClassifierRuleBasePath()+"//RuleBaseTmp.txt").collect()
     
      // Read the rule base
			val nFeatures = dataBase.getNumVariables()
      var ants: Array[Byte] = Array.fill(nFeatures)(0.toByte)
      var consequent: Byte = 0
      var weight: Double = 0.0
       
      for (line <- ruleBaselines){
        var array = line.split(" ")
        var feature: Int = 0
        array.foreach(data => {
            if (feature < nFeatures){
              ants(feature) = data.toByte
            }
            else if (feature == nFeatures){
              // Get consequent
        	    consequent = data.toByte
            }
            else {
              weight = data.toDouble
            }
            feature = feature + 1})
      	
      	// Build fuzzy rule
      	ruleBase = ruleBase :+ new FuzzyRule(ants, consequent, weight, dataBase.getNumClasses())
      }
			
			val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
      fs.delete(new Path(Mediator.getClassifierRuleBasePath()+"//RuleBaseTmp.txt"),false)
			
		}
		catch{
		  case e: Exception => {
	      System.err.println("\nHITS MAPPER: ERROR READING RULE BASE\n")
        e.printStackTrace()
        System.exit(-1)}
		}
		
		ruleBase
	}
	
	/**
	 * Reads all the objects (variables, class labels, etc.) and parameters of this class (corresponding to the learning algorithm)  from the specified configuration file
	 * @param conf Configuration instance where all the objects and parameters of this class are stored (in the future this configuration instance will be considered)
	 * @throws Base64DecodingException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	def readConfiguration (conf: SparkConf) {

	  /**
		 *  Read basic configuration about Learner
		 */
		headerPath = conf.get(DATASET_HEADER_PATH)
		learnerInputPath = conf.get(LEARNER_INPUT_PATH_FIELD)
		learnerOutputPath = conf.get(LEARNER_OUTPUT_PATH_FIELD)
		val cs = conf.get(COST_SENSITIVE_FIELD)
		if(cs.contentEquals("1"))
		  costSensitive = true
		else
		  costSensitive = false
				
		/**
		 *  Read basic configuration about Classifier
		 */
		classifierInputPath = conf.get(CLASSIFIER_INPUT_PATH_FIELD)
		classifierOutputPath = conf.get(CLASSIFIER_OUTPUT_PATH_FIELD)
		classifierDatabasePath = conf.get(CLASSIFIER_DATABASE_PATH_FIELD)
		classifierRuleBasePath = conf.get(CLASSIFIER_RULE_BASE_PATH_FIELD)
		//hdfsLocation = conf.get(HDFS_LOCATION_FIELD)
		var frmStr = conf.get(FRM_FIELD)
		if(frmStr.contentEquals("wr") || frmStr.contentEquals("winningrule") || frmStr.contentEquals("winning_rule")){
		  frm = KnowledgeBase.FRM_WINNING_RULE
		}
		else{
		  frm = KnowledgeBase.FRM_ADDITIVE_COMBINATION
		}
		
		/**
		 *  Read basic configuration about Chi
		 */
		numLinguisticLabels = conf.get(NUM_LINGUISTIC_LABELS_FIELD).toByte

		/**
		 *  Read basic configuration about genetic algorithm
		 */
		populationSize = conf.get(POPULATION_FIELD).toInt
		numEvaluations = conf.get(EVALUATIONS_FIELD).toInt
		alpha = conf.get(ALPHA).toDouble
		init_seed = conf.get(INIT_SEED).toLong
		crossValidation = conf.get(CROSS_VALIDATION).toByte
		
		configuration = conf

	}

	/**
	 * Stores classifier database path in the configuration file
	 * @param newDatabasePath classifier database path
	 */
	def saveClassifierDataBasePath (newDatabasePath: String){

		configuration.set(CLASSIFIER_DATABASE_PATH_FIELD, newDatabasePath)
		classifierDatabasePath = newDatabasePath

	}

	/**
	 * Stores classifier input path in the configuration file
	 * @param newInputPath classifier input path
	 */
	def saveClassifierInputPath (newInputPath: String){

		configuration.set(CLASSIFIER_INPUT_PATH_FIELD, newInputPath)
		classifierInputPath = newInputPath

	}

	/**
	 * Stores classifier output path in the configuration file
	 * @param newOutputPath classifier output path
	 */
	def saveClassifierOutputPath (newOutputPath: String){

		configuration.set(CLASSIFIER_OUTPUT_PATH_FIELD, newOutputPath)
		classifierOutputPath = newOutputPath

	}

	/**
	 * Stores classifier rule base path in the configuration file
	 * @param newRuleBasePath classifier rule base path
	 */
	def saveClassifierRuleBasePath (newRuleBasePath: String){

		configuration.set(CLASSIFIER_RULE_BASE_PATH_FIELD, newRuleBasePath)
		classifierRuleBasePath = newRuleBasePath

	}
	
	/**
	 * Stores dataset header path in the configuration file
	 * @param newDatasetHeader dataset header path
	 */
	def saveHeaderPath (newSatasetHeader: String){

		configuration.set(DATASET_HEADER_PATH, newSatasetHeader)
		headerPath = newSatasetHeader

	}	
	
	/**
	 * Stores learner input path in the configuration file
	 * @param newInputPath learner test input path
	 */
	def saveLearnerInputPath (newInputPath: String){

		configuration.set(LEARNER_INPUT_PATH_FIELD, newInputPath)
		learnerInputPath = newInputPath

	}

	/**
	 * Stores learner output path in the configuration file
	 * @param newOutputPath learner output path
	 */
	def saveLearnerOutputPath (sc: SparkContext, newOutputPath: String){
	  
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	
		configuration.set(LEARNER_OUTPUT_PATH_FIELD, newOutputPath)
		learnerOutputPath = newOutputPath
	
		//Create to Learner Output Path
		val conf = sc.hadoopConfiguration
		conf.set(LEARNER_OUTPUT_PATH_FIELD, newOutputPath)
		val fs = FileSystem.get(conf)
		if(!fs.exists(new Path(Mediator.getLearnerOutputPath())))
		  fs.mkdirs(new Path(Mediator.getLearnerOutputPath()))
		if(!fs.exists(new Path(Mediator.getLearnerOutputPath()+"//Train")))
		  fs.mkdirs(new Path(Mediator.getLearnerOutputPath()+"//Train"))
	}

	/**
	 * Stores learner rule base size (number of rules) in the configuration file
	 * @param size learner rule base size (number of rules)
	 */
	def saveLearnerRuleBaseSize (size: Byte){

		configuration.set(LEARNER_RULE_BASE_SIZE_FIELD, Integer.valueOf(size).toString())
		learnerRuleBaseSize = size

	}
	
	/**
	 * Stores Temporal RuleBase (collection of rules) in a file
	 * @param ruleBase: Rule Base (collection of rules)
	 * @param filepath: path and name of file where it is stored 
	 */
	def saveTmpRBFile(ruleBase: Array[FuzzyRule], filepath: String, sc: SparkContext, printStdOut: Boolean = false): Unit = {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}    
    import java.net.URI
    import java.io.{BufferedWriter, OutputStreamWriter}
    
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(new URI(Mediator.getLearnerRuleBasePath()), conf)
    val textPath = new Path(filepath)
    var bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath))) 	    

	  ruleBase.foreach { rule => bwText.write(rule.getAntecedent().mkString(" ") 
	      + " " + rule.getClassIndex() 
	      + " " + rule.getRuleWeight() + "\n")}
	  
    bwText.close()
  }

	/**
	 * Sets a new configuration
	 * @param newConfiguration configuration object
	 */
	def setConfiguration (newConfiguration: SparkConf) {
		
	  configuration = newConfiguration
		
	}

	/**
	 * Stores CHI algorithm parameters in the configuration file
	 * @param inputFilePath input file path
	 * @return true: If cost sensitive function is allowed
	 */
	def storeChiParameters (maxParams: Int, inputFilePath: String): Array[Boolean] = {

	  //Reading file
    val lines = Source.fromFile(inputFilePath).getLines.toArray
    var CS_RS = Array.fill(2)(false)
		var st: StringTokenizer = null
		var i = 0
		var correct_params = true
		for(buffer <- lines){
			st = new StringTokenizer (buffer, "=")
			var param = st.nextToken().toLowerCase()
		  if(param.compareTo("inference") == 0){
			  configuration.set(param,st.nextToken().toLowerCase())
			  i = i + 1
		  }
			if(param.compareTo("num_linguistic_labels") == 0){
			  configuration.set(param,st.nextToken().toLowerCase())
			  i = i + 1
		  }
			if(param.compareTo("cost_sensitive") == 0){
			  val cost_sensitive = st.nextToken().toLowerCase()
			  configuration.set(param,cost_sensitive)
			  i = i + 1
			  if(cost_sensitive.compareTo("1") == 0)
           CS_RS(0) = true
           
        if(cost_sensitive.compareTo("1") != 0 || cost_sensitive.compareTo("0") != 0)
          correct_params = false
		  }
		  else if(param.compareTo("num_individuals") == 0){				  
			   configuration.set(param,st.nextToken().toLowerCase())
			   i = i + 1
			}
		  else if(param.compareTo("num_evaluations") == 0){
		    val num_evaluations = st.nextToken().toLowerCase()
			  configuration.set(param,num_evaluations)
			  i = i + 1
			  if(num_evaluations.toInt > 0)
			    CS_RS(1) = true
			    
			  if(num_evaluations.toInt < 0)
			    correct_params = false
			}
			else if(param.compareTo("alpha") == 0){
			  val alpha = st.nextToken().toLowerCase()
			  configuration.set(param,alpha)
			  i = i + 1
			  if(alpha.toDouble < 0.0 || alpha.toDouble > 1.0)
			    correct_params = false
			}
		  else if(param.compareTo("init_seed") == 0){				  
			   configuration.set(param,st.nextToken())
			   i = i + 1
			}
			else if(param.compareTo("cross_validation") == 0){			
			   val aux = st.nextToken().toLowerCase()
			   configuration.set(param,aux)
			   if(aux.toByte > 0)
			     optionCrossValidation = true
			   i = i + 1
			}
		}

    if(i != maxParams || correct_params){
      throw new IllegalArgumentException("ERROR IN FILE "+inputFilePath+"\n Number of paramaters is always 4, like this:\n"
                           +" inference={0: FRM_WINNING_RULE = 0, 1:FRM_ADDITIVE_COMBINATION}\n"
                           +" num_linguistic_labels={3, 5,...}\n" 
                           +" cost_sensitive={0: Without Cost Sensitive, 1: Function Cost Sensitive}\n" 
                           +" num_individuals={1,..., 50,...}\n"
                           +" num_evaluations={1,...}\n"
                           +" alpha={0.0,...,1.0}\n"
                           +" init_seed={0,...}\n"
                           +" cross_validation={0: No CrossValidation, 1,...}\n")
      
    }
  
    CS_RS
	}

	/**
	 * Returns whether the rule weight computation is cost-sensitive (only for binary datasets)
	 * @true if the rule weight computation is cost-sensitive
	 */
	def useCostSensitive (): Boolean ={
		costSensitive
	}
	
}