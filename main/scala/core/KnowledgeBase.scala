package core

import org.apache.spark.SparkContext

import org.apache.hadoop.io.Writable

import java.io.{DataInput, DataOutput}

import utils.{CounterRules, ConsequentPart}

object KnowledgeBase {
  
  final val FRM_WINNING_RULE: Byte = 0
	final val FRM_ADDITIVE_COMBINATION: Byte = 1
  final val RW_NONE: Byte = 0;
  final val RW_CF: Byte = 1;
  final val RW_PCF: Byte = 2;
}

class KnowledgeBase extends Writable with Serializable{
  
  /**
   * Rule base
   */
  private var ruleBase: Array[FuzzyRule] = null
  
  private var dataBase: DataBase = null
  
  /**
   * Is only about info
   */
  private var counterRules: Array[CounterRules] = null 
  private var counter: CounterRules = null
  
  def initCounterRules(rules: Array[Int]){
    counter = new CounterRules(rules(0), rules(1)) 
  }
  
  def addCounterRules(rules: CounterRules){
    if(counterRules == null){
      counterRules = Array[CounterRules]()
      counterRules = counterRules :+ rules
    }else{
      counterRules = counterRules :+ rules
    }
  }
  
  def addCounterRules(rules: Array[CounterRules]){
    if(counterRules == null){
      counterRules = Array[CounterRules]()
      counterRules = rules
    }else{
      counterRules = counterRules ++ rules
    }
  }
  
  def getCounter(): CounterRules = counter
  
  def getCounterRules(): Array[CounterRules] = counterRules
  
  def eraseCounterRules() {
    counterRules = null
  }
  
  def this (db: DataBase, cr: CounterRules){
  
    this()
		ruleBase = Array[FuzzyRule]()
		dataBase = db
		counter = cr

	}
  
  /*TMP**/
  
  /**
   * Is only about info
   * /
  private var counterClass: Array[Array[Int]] = null 
  
  def addCounterClass(counter: Array[Int]){
    if(counterClass == null){
      counterClass = Array[Array[Int]]()
      counterClass = counterClass :+ counter
    }else{
      counterClass = counterClass :+ counter
    }
  }
  
  def getCounterClass():Array[Array[Int]] = counterClass
  
  def eraseCounterClass() {
    counterClass = null
  }
  / *TMP**/
  
  def this (size: Byte){
      
    this()
    this.ruleBase = new Array[FuzzyRule](size)
      
  }
  
  def this (db: DataBase){
  
    this()
		ruleBase = Array[FuzzyRule]()
		dataBase = db
		
	}
  
  def this (db: DataBase, rb: Array[FuzzyRule]/*, tmp cc: Array[Array[Int]]*/){
  
    this()
		ruleBase = rb.clone()
		dataBase = db
		//counterClass = cc.clone()

	}
  
  //Tmp
  /*def this (db: DataBase, cr: Array[Array[Int]]){
  
    this()
		ruleBase = Array[FuzzyRule]()
		dataBase = db
		//counterClass = cc.clone 
		//counterRules = cr.clone
	}*/

  override def clone(): KnowledgeBase = {
		var kb = new KnowledgeBase(this.dataBase, this.counter)
		for (fr <- ruleBase){
			kb.addFuzzyRule(fr.clone())
		}
		kb
	}
  
	def setDataBase(db: DataBase){
		this.dataBase = db
	}
  
  /**
   * Creates a new fuzzy rule from an instance
   * @param instanceStr string of the instance used to create a single rule
   */
  def addFuzzyRule (newFuzzyRule: FuzzyRule){

    if(ruleBase == null){
      ruleBase = Array[FuzzyRule]()
    }
    ruleBase = ruleBase :+ newFuzzyRule
  }
  
  /**
	 * Obtains the index of the maximum value of the array
	 * @param degrees array (contains association degrees for the classes)
	 * @return the class index corresponding to the highest association degree
	 */
	def maxIndex(degrees: Array[Double]): Byte ={
	  
		var classIndex: Byte = 0
		var max = degrees(0)
		for (i <- 1 to (degrees.length - 1)){
			if (degrees(i) > max){
				max = degrees(i)
				classIndex = i.toByte
			}
		}
		classIndex
		
	}
  
  /**
   * Classifies an example
   * @param frm fuzzy reasoning method to be used (0: winning rule, 1: additive combination)
   * @param example input example
   * @return predicted class
   */
  def classify (frm: Byte, example: Array[String]): Byte = {
    
  	if (frm == KnowledgeBase.FRM_WINNING_RULE)
  		FRM_WR(example)(0).toByte
  	else
  		FRM_AC(example)(0).toByte
  		
  }
  
  /**
	 * Classifies an example
	 * @param frm fuzzy reasoning method to be used (0: winning rule, 1: additive combination)
	 * @param example input example
	 * @return predicted class
	 */
  def classifyOK (frm: Byte, example: Array[Double]): Byte = {
  
		if (frm == KnowledgeBase.FRM_WINNING_RULE)
			FRM_WR_Class(example) 
		else
			FRM_AC_Class(example)
			
	}
  
  /**
	 * Classifies an example
	 * @param frm fuzzy reasoning method to be used (0: winning rule, 1: additive combination)
	 * @param example input example
	 * @return predicted class
	 * /
	def classifyDegrees (frm: Byte, example: Array[String]): Array[Double] = {
	  
		if (frm == KnowledgeBase.FRM_WINNING_RULE)
			FRM_WR(example)
		else
			FRM_AC(example)
			
	}*/
	
	/**
   * Classifies an example
   * @param frm fuzzy reasoning method to be used (0: winning rule, 1: additive combination)
   * @param individual of the chromosome (0: false, 1: true)
   * @param example input example
   * @return predicted class
   */
  def classifyRS (frm: Byte, individual: Array[Byte], example: Array[String]): Byte = {
    
  	if (frm == KnowledgeBase.FRM_WINNING_RULE)
  		FRM_WR_Individual(individual, example)(0).toByte
  	else
  		FRM_AC(example)(0).toByte
  		
  }
	
	/**
   * Returns of the occurrences of each class label
   * @return the occurrences of each class label
   */
	def counterClassLabels(): Array[Int] = {
	  
	  var counter = Array.fill(dataBase.getNumClasses())(0)
	  for (rule <- ruleBase){
	    counter(rule.getClassIndex()) = (counter(rule.getClassIndex()) + 1) 
	  }
	  counter
	  
	}
  
  /**
   * Additive Combination Fuzzy Reasoning Method
   * @param example input example
   * @return a double array where [0] is the predicted class index and [1] is the confidence degree
   */
  private def FRM_AC (example: Array[String]): Array[Double] = {
  	
    var output = Array.fill(dataBase.getNumClasses()+1)(0.0)
		output(0) = dataBase.getMostFrequentClass() // Default class
		/*for (i <- 1 to (output.length - 1))
			output(i) = 0.0*/ // Default confidence

		var classDegree = Array.fill(dataBase.getNumClasses())(0.0)
  	
  	var degree = 0.0

  	// Compute the confidence of each class
  	for (rule <- ruleBase) {
  		degree = this.computeAssociationDegree(example, rule)
  		classDegree(rule.getClassIndex()) += degree
  	}
    
    for (i <- 1 to (output.length - 1)) //Normalize??
			output(i) = classDegree(i-1) 
    
    // Get the class with the highest confidence
  	for (i <- 0 to (classDegree.length - 1)) {
  		if (classDegree(i) > output(1)) {
  			output(0) = i
  			output(1) = classDegree(i)
  		}
  	}
  	
  	output(0) = maxIndex(classDegree)
		  	
  	output
  	
  }
  
  /**
   * Winning Rule Fuzzy Reasoning Method
   * @param example input example
   * @return a double array where [0] is the predicted class index and [1] is the confidence degree
   */
  private def FRM_WR (example: Array[String]): Array[Double] = {
  	
  	var output = Array.fill(dataBase.getNumClasses()+1)(0.0)
  	output(0) = dataBase.getMostFrequentClass() // Default class
  	/*for (i <- 1 to (output.length - 1))
			output(i) = 0.0*/ // Default confidence
  	var degree = 0.0
  	
  	// Get the class with the rule with highest association degree
		for (rule <- ruleBase) {
			degree = this.computeAssociationDegree(example,rule)
			if (output(rule.getClassIndex()+1) < degree)
				output(rule.getClassIndex()+1) = degree
		}
  	
		//Truncation
		var indexMax: Int = 1
		var max: Double = output(1)
		for (i <- 2 to (output.length - 1)){
			if (output(i) > max){
				max = output(i)
				output(indexMax) = 0
				indexMax = i
			}else{
				output(i) = 0
			}
		}
		//indexMax-1 ; //
		output(0) = indexMax-1 //maxIndex(output)-1; //output has one more element than number of classes
  	
  	output
  	
  }
  
  /**
   * Winning Rule Fuzzy Reasoning Method
   * @param example input example
   * @return a double array where [0] is the predicted class index and [1] is the confidence degree
   */
  private def FRM_WR_Individual (individual: Array[Byte], example: Array[String]): Array[Double] = {
  	
  	var output = Array.fill(dataBase.getNumClasses()+1)(0.0)
  	output(0) = dataBase.getMostFrequentClass() // Default class
  	var degree = 0.0
  	
  	// Get the class with the rule with highest association degree
		for (i <- 0 to (ruleBase.length - 1)) {
		  if(individual(i) == 1){
		    val rule = ruleBase(i)
    		degree = this.computeAssociationDegree(example,rule)
    		if (output(rule.getClassIndex()+1) < degree)
    			output(rule.getClassIndex()+1) = degree
		  }
		}
  	
		//Truncation
		var indexMax: Int = 1 //default class = 0
		var max: Double = output(1)
		for (i <- 2 to (output.length - 1)){
			if (output(i) > max){
				max = output(i)
				output(indexMax) = 0
				indexMax = i
			}else{
				output(i) = 0
			}
		}
		//indexMax-1 ; //
		output(0) = indexMax-1 //maxIndex(output)-1; //output has one more element than number of classes
  	
  	output
  	
  }
  
  /**
	 * Winning Rule Fuzzy Reasoning Method
	 * @param example input example
	 * @return a double array where [0] is the predicted class index and [1] is the confidence degree
	 */
	private def FRM_WR_Class (example: Array[Double]): Byte ={

		var output: Byte = dataBase.getMostFrequentClass() // Default class
		var degree, max_degree: Double = 0.0
	  
		// Get the class with the rule with highest association degree
		//for (FuzzyRule rule:ruleBase) {
		for (i <- 0 to (ruleBase.length - 1)) {
			degree = this.computeAssociationDegreeD(example, ruleBase(i))
			if (max_degree < degree){
				max_degree = degree
				output = ruleBase(i).getClassIndex() 
			}
		}
		output

	}

	/**
	 * Winning Rule Fuzzy Reasoning Method
	 * @param example input example
	 * @return a double array where [0] is the predicted class index and [1] is the confidence degree
	 */
	private def FRM_AC_Class (example: Array[Double]): Byte ={
		dataBase.getMostFrequentClass() // Default class
	}
	
	/**
	 * Returns the association degree of the input example with this rule
	 * @param example input example
	 * @return association degree of the input example with this rule
	 */
	def computeAssociationDegree (example: Array[String], r: FuzzyRule): Double = {
		computeMatchingDegree(example,r)*r.getRuleWeight()
	}

	def computeMatchingDegree (example: Array[String], r: FuzzyRule): Double = {

		var matching: Double = 1.0
		var i: Int = 0
		while((i < example.length) && (matching > 0)){
			// If it is a nominal value and it is not equal to the antecedent, then there is no matching
			if (dataBase.get(i).isInstanceOf[NominalVariable]){
				if (!(dataBase.get(i).asInstanceOf[NominalVariable]).getNominalValue(r.getAntecedent(i.toByte)).contentEquals(example(i)))
					return 0.0
			}
			else{
				matching *= (dataBase.get(i).asInstanceOf[FuzzyVariable]).getMembershipDegree(r.getAntecedent(i.toByte), example(i).toDouble)
			}
			i = i + 1
		}
		matching

	}
	
	/**
	 * Returns the association degree of the input example with this rule
	 * @param example input example
	 * @return association degree of the input example with this rule
	 */
	def computeAssociationDegreeD (example: Array[Double], r: FuzzyRule): Double = {
		computeMatchingDegreeD(example,r)*r.getRuleWeight()
	}

	def computeMatchingDegreeD (example: Array[Double], r: FuzzyRule): Double = {

		var matching: Double = 1.0
		var i: Int = 0
		while ((i < example.length)&&(matching > 0)){
			// If it is a nominal value and it is not equal to the antecedent, then there is no matching
			if (dataBase.get(i).isInstanceOf[NominalVariable]){
				var valor = example(i).toInt
				if (!(dataBase.get(i).asInstanceOf[NominalVariable]).getNominalValue(r.getAntecedent(i.toByte)).contentEquals(String.valueOf(valor))){
					return 0.0
				}
			}
			else
				matching *=  (dataBase.get(i).asInstanceOf[FuzzyVariable]).getMembershipDegree(r.getAntecedent(i.toByte), example(i))
			i = i + 1
		}
		
		matching
		
	}	

	def getDataBase(): DataBase = dataBase

	def getNumVariables(): Int = dataBase.getNumVariables()
	
	def getSizeRuleBase(): Int = ruleBase.size

	/**
   * Returns the rule base of this classifier
   * @return rule base of this classifier
   */
  def getRuleBase (): Array[FuzzyRule] = ruleBase

	def getRule(i: Int): FuzzyRule = ruleBase(i)

	override def toString(): String = {
	  
		var output: String = ""
		output += "DATABASE\n;"
		output += this.dataBase.toString()+"\n"
		for (id <- 0 to (ruleBase.length - 1)){
			output+= "Rule ("+id+"): "
			// Write antecedents
			output += ruleBase(id).toString(dataBase)
			output += "\n\n"
		}
		
		return output
	}
	
  /**
   * Remove the rules with individual equal to 0
   * @param individual of the chromosome
   */
	def removeRules(individual: Array[Byte]){
	
	  var ruleBaseTmp = Array[FuzzyRule]()
	  var counterRulesTmp = Array.fill(dataBase.getNumClasses())(0)
	  for(i <- 0 to (individual.length - 1)){
	    if(individual(i) == 1){
	      ruleBaseTmp =  ruleBaseTmp :+ ruleBase(i)
	    }else{
	      counterRulesTmp(ruleBase(i).getClassIndex()) = counterRulesTmp(ruleBase(i).getClassIndex()) + 1 
	    }
	  }
	  counter.reduceRules(counterRulesTmp)
	  ruleBase = ruleBaseTmp
	}
	
	def size(): Int = {
		ruleBase.length
	}
	
	override def readFields(in: DataInput) /*throws IOException*/ {

		dataBase = new DataBase()
		dataBase.readFields(in)
		var size = in.readInt()
		ruleBase = Array[FuzzyRule]()
		for (i <- 0 to (size - 1)){
			var fr = new FuzzyRule()
			fr.readFields(in)
			ruleBase = ruleBase :+ (fr)
		}

	}

	override def write(out: DataOutput) /*throws IOException */{
		dataBase.write(out)		
		out.writeInt(ruleBase.length)
		for (fr <- ruleBase){
			fr.write(out)
		}
	}
	
	def saveDBFile(filepath: String, sc: SparkContext){
  
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}    
    import java.net.URI
    import java.io.{BufferedWriter, OutputStreamWriter}
    
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)//FileSystem.get(new URI(Mediator.getLearnerDataBasePath()), conf)
    val textPath = new Path(filepath)
    var bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath,true)))
			
    bwText.write(dataBase.getDataBase().mkString("\n").toString())
		bwText.write("\nClass:\n" + " "+ dataBase.getClassLabels().mkString(", ").toString() + "\n")
		
		var classCost = Array.fill(2)(1.0)
		var numExamples: Array[Long] = dataBase.getClassNumExamples()
		if (numExamples(0) > numExamples(1)){
			classCost(0) = 1.0
			classCost(1) = (numExamples(0).toDouble) / (numExamples(1).toDouble)
		}
		else if (numExamples(0) < numExamples(1)){
			classCost(0) = (numExamples(1).toDouble) / (numExamples(0).toDouble)
			classCost(1) = 1.0
		}
    bwText.write("\nIR:\n" + " "+ classCost.deep.mkString(", ").toString() + "\n")
    
    bwText.close()
  }

	def saveRBFile(filepath: String, sc: SparkContext, printStdOut: Boolean = false): Unit = {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}    
    import java.net.URI
    import java.io.{BufferedWriter, OutputStreamWriter}
    
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(new URI(Mediator.getLearnerRuleBasePath()), conf)
    val textPath = new Path(filepath)
    var bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath,true)))
     	        
		bwText.write("\nRULE BASE:\n\n")
		if (printStdOut){
		  System.out.println("\nRULE BASE:\n")
		}
	  
	  var id: Int = 0
	  for (rule <- ruleBase){ 
	    bwText.write("Rule ("+id.toString()+"): IF ")
	    
	    // Write antecedents
			for (i <- 0 to (rule.getAntecedent().length - 2)){
			  
				bwText.write(dataBase.getDataBase()(i).getName()+" IS ")
				if (printStdOut){
					System.out.print(dataBase.getDataBase()(i).getName()+" IS ")
				}
				if (dataBase.getDataBase()(i).isInstanceOf[NominalVariable]){
					bwText.write((dataBase.getDataBase()(i).asInstanceOf[NominalVariable]).getNominalValue(rule.getAntecedent()(i))+" AND ")
					if (printStdOut){
						System.out.print((dataBase.getDataBase()(i).asInstanceOf[NominalVariable]).getNominalValue(rule.getAntecedent()(i))+" AND ")
					}
				}
				else {
					bwText.write("L_" + rule.getAntecedent()(i).toString()+" AND ")
					if (printStdOut){
						System.out.print("L_" + rule.getAntecedent()(i).toString()+" AND ")
					}
				}
	  	}
	    
	    // Write the last antecedent
			bwText.write(dataBase.getDataBase()(rule.getAntecedent().length - 1).getName()+" IS ")
			if (printStdOut){
				System.out.print(dataBase.getDataBase()(rule.getAntecedent().length - 1).getName()+" IS ")
			}
			if (dataBase.getDataBase()(rule.getAntecedent().length - 1).isInstanceOf[NominalVariable]){
				bwText.write(dataBase.getDataBase()(rule.getAntecedent().length - 1).asInstanceOf[NominalVariable].getNominalValue(rule.getAntecedent()(rule.getAntecedent().length - 1)))
				if (printStdOut){
					System.out.print(dataBase.getDataBase()(rule.getAntecedent().length - 1).asInstanceOf[NominalVariable].getNominalValue(rule.getAntecedent()(rule.getAntecedent().length - 1)))
				}
			}
			else {
				bwText.write("L_" + rule.getAntecedent()(rule.getAntecedent().length - 1).toString())
				if (printStdOut){
					System.out.print("L_" + rule.getAntecedent()(rule.getAntecedent().length - 1).toString())
				}
			}

			// Write the class and rule weight
			bwText.write(" THEN CLASS = " + dataBase.getClassLabel(rule.getClassIndex()))
			bwText.write(" WITH RW = " + rule.getRuleWeight().toString()+"\n\n")
			if (printStdOut){
				System.out.print(" THEN CLASS = " + dataBase.getClassLabel(rule.getClassIndex()))
				System.out.print(" WITH RW = " + rule.getRuleWeight()+"\n\n")
			}

			id = id + 1
	  }
		
    bwText.close()
  }
}