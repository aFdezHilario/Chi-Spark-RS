package core

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

import java.util.StringTokenizer
import java.io.{DataInput, DataOutput, IOException}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Writable

/**
 * It contains the Data Base.
 *
 * @author Eva M. Almansa
 * @version 1.0
 */
class DataBase extends Writable with Serializable{

  	/**
  	 * Fuzzy / Nominal sets
  	 */
  	private var dataBase: Array[Variable] = null
  	
  	/**
  	 * Class labels
  	 */
  	private var classLabels: Array[String] = null
  	
  	/**
  	 * Number of examples of each class
  	 */
  	private var classNumExamples: Array[Long] = null
  	
  	/**
  	 * Most frequent class
  	 */
  	private var classMostFrequent: Byte = 0
  
  	/**
  	 * Number of class labels
  	 */
  	private var numClassLabels: Byte = 0
  	
    /**
  	 * Number of linguistic labels (fuzzy sets) considered for all fuzzy variables
     */
	  private var numLinguisticLabels: Byte = 0
	  
	  /**
  	 * Position of the class labels: First or last position
  	 */
	  private var posClassLabels: Byte = 0
  	
  	/**
  	 * Constructor
  	 * @param size Number of variables in the dataset
  	 */
  	def this (size: Int){
  	  this()
  		this.dataBase = new Array[Variable](size)
  	}
  	
  	/**
  	 * Reads header file and generates fuzzy variables
  	 * @param sc SparkContext
  	 * @param filePath file path
  	 * @param nLinguisticLabels number of linguistic labels
  	 */
  	def this (sc: SparkContext, filePath: String) {
  
  	  this()
  	  
  		try{
  		  
  		  //Reading header file
  		  val lines = sc.textFile(filePath, 1).persist().collect
  		  
  		  var buffer: String = null
  			var st: StringTokenizer = null
        
  			var output: String = ""
  			var variablesTmp = new ListBuffer[Variable]
  			var classNumExamples: Array[Long] = null
  
  			numLinguisticLabels = Mediator.getNumLinguisticLabels()
  			var index = 0
  			while(index < lines.length){
  			  
  			  var line = lines(index)
  			  if (line.isEmpty()){
  				  throw new SecurityException("\nERROR READING HEADER FILE: It is not permited empty lines\n")
  			  }
  			  
  				buffer = line.replaceAll(", ", ",")//.split(",")
  				st = new StringTokenizer (buffer)
  				var field = st.nextToken()
  				
  				// Attribute
  				if (field.contentEquals("@attribute")){
  
  					// Attribute name
  					var attribute = st.nextToken()
  					var name, typeData: String = null
  					
  					// Check format
  					if (!attribute.contains("{") && !attribute.contains("[")) {
  						name = attribute
  						while (st.hasMoreTokens() && !attribute.contains("{") && !attribute.contains("[")){
  							typeData = attribute
  							attribute = st.nextToken()
  						}
  						if (!attribute.contains("{") && !attribute.contains("[")) {
  							throw new SecurityException("\nERROR READING HEADER FILE: Values are not specified\n")
  						}
  					}
  					else if (attribute.contains("[")) {
  						throw new SecurityException("\nERROR READING HEADER FILE: Invalid attribute name\n")
  					}
  					else {
  						name = attribute.substring(0,attribute.indexOf("{"))
  						typeData = name
  					}
  					
  					// Nominal attribute
  					if (typeData == name && attribute.contains("{")){
  						// Get nominal values
  						attribute = attribute.substring(attribute.indexOf("{")+1)
  						st = new StringTokenizer (attribute,"{, ")
  						var nominalValues = Array[String]()
  						var end = false
  						while(!end){
    						while (st.hasMoreTokens()){
    						  var aux = st.nextToken()
    						  if(aux.contains("}")){
    						    end = true
    						    aux = aux.replaceAll("}", "")
    						  } 						  
    							nominalValues = nominalValues :+ aux
    						}
    						
    						if(!end){
    						   throw new SecurityException("\nERROR READING HEADER FILE: Invalid structure in Nominal Variable. Check of variable with name="+name+"\n")
    						}
  						}
  						// Build a new nominal variable
  						var newVariable = new NominalVariable(name)
  						newVariable.setNominalValues(nominalValues)
  
  						variablesTmp += newVariable
  						  
  					}
  					// Numeric attribute
  					else if (attribute.contains("[")){
  					  
  						// Check format
  						if (typeData != name && !typeData.toLowerCase().contentEquals("integer") 
  								&& !typeData.toLowerCase().contentEquals("real")) {
  							throw new SecurityException("\nERROR READING HEADER FILE: Invalid attribute type: '"+typeData+"'\n")
  						}
  						else if (typeData == name && !attribute.toLowerCase().contains("integer") 
  								&& !attribute.toLowerCase().contains("real")){
  							throw new SecurityException("\nERROR READING HEADER FILE: No attribute type is specified\n")
  						}
  
  						// Get upper and lower limits
  						st = new StringTokenizer (attribute.substring(attribute.indexOf("[")+1),"[], ")
  
  						var lowerLimit = st.nextToken().toDouble
  						var upperLimit = st.nextToken().toDouble
  
  						// Integer attribute
  						if (attribute.toLowerCase().contains("integer")){
  
  							// If the number of integer values is less than the number of
  							// linguistic labels, then build a nominal variable
  							if ((upperLimit - lowerLimit + 1) <= numLinguisticLabels){
  								var nominalValues = Array.fill(upperLimit.toInt - lowerLimit.toInt+1)("".toString)
  								for (i <- 0 to (nominalValues.length - 1))
  									nominalValues(i) = (lowerLimit+i).toString
  								var newVariable = new NominalVariable(name)
  								newVariable.setNominalValues(nominalValues)
  								variablesTmp += newVariable
  							}
  							else {
  								var newVariable = new FuzzyVariable(name)
  								newVariable.buildFuzzySets(lowerLimit,upperLimit,numLinguisticLabels)
  								variablesTmp += newVariable
  							}
  
  						}
  						// Real attribute
  						else {
  							var newVariable = new FuzzyVariable(name)
  							newVariable.buildFuzzySets(lowerLimit,upperLimit,numLinguisticLabels)
  							variablesTmp += newVariable
  						}
  						 
  					}
  					else {
  						throw new SecurityException("\nERROR READING HEADER FILE: Invalid format\n")
  					}
  
  				}
  				else if (field.contentEquals("@outputs") || field.contentEquals("@output")){
  
  					st = new StringTokenizer (st.nextToken(),", ");
  					if (st.countTokens()>1){
  						throw new SecurityException("\nERROR READING HEADER FILE: This algorithm does not support multiple outputs\n")
  					}
  					output = st.nextToken()
  				}
  				else if (field.toLowerCase().contentEquals("@numinstancesbyclass")){
  
  					st = new StringTokenizer (st.nextToken(), ", ")
  					classNumExamples = Array.fill(st.countTokens())(0.toLong)
  					
  					var i = 0
  					while (st.hasMoreTokens()){
  						classNumExamples(i) = st.nextToken().toLong
  						i = i.+(1)
  					}
  				}
  				
  				index = index + 1
  			}
        
  			if (classNumExamples == null){
  				throw new IllegalArgumentException("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n")
  			}
        
  			// Remove output attribute from variable list and save it as the class
  			var iterator = variablesTmp.iterator
  			var i: Int = 0
  			var found = false
  			while (iterator.hasNext && !found){
  				var variable = iterator.next().asInstanceOf[Variable]
  				if (output.contentEquals(variable.getName())){
  					// Save class labels
  					classLabels = variable.asInstanceOf[NominalVariable].getNominalValues()
  					// Remove from the list
  					variablesTmp.remove(i)
  					found = true
  					posClassLabels = i.toByte
  				}
  				i = i.+(1)
  			}
        
  			if (!found){
  				throw new SecurityException("\nERROR READING HEADER FILE: The name of output is not correct, it is necessary samething like @output <name> or @outputs <name>\n")
  			}
  			
  			if ((posClassLabels != 0) && (posClassLabels != variablesTmp.length)){
  				throw new SecurityException("\nERROR READING HEADER FILE: The Class Label position is not correct, the position can only be in the first or last place of the other variables.\n")
  			}
  			
  			dataBase = new Array[Variable](variablesTmp.length)
  			for (i <- 0 to (variablesTmp.length - 1)){
  				dataBase(i) = variablesTmp(i)
  			}
  
  			// Save the number of examples of each class
  			saveClassNumExamples(classNumExamples.toArray)
 
  		}catch {
  		  case e: Exception => {
          System.err.println("ERROR WRITING RULE BASE: \n")
          e.printStackTrace()
          System.exit(-1)}
  		}
  	}
  	
  	/**
  	 * Stores the number of examples of each class in the configuration file
  	 * @param numExamplesByClass number of examples of each class
  	 * @throws IOException 
  	 */
  	private def saveClassNumExamples (numExamplesByClass: Array[Long]) /*throws IOException*/ {
  
  		// Compute the most frequent class
  		var classNumExamplesArray = Array.fill(numExamplesByClass.length)(0.toLong)
  		var maxValue: Long = -1
  		var mostFrequentClass: Byte = 0
  		var i: Byte = 0
  		
  		for (element <- numExamplesByClass) {
  			classNumExamplesArray(i) = element
  			if (classNumExamplesArray(i) > maxValue){
  				maxValue = classNumExamplesArray(i)
  				mostFrequentClass = i
  			}
  			i = (i + 1).toByte
  		}
  		
  		numClassLabels = classLabels.length.toByte
  		classNumExamples = classNumExamplesArray
  		classMostFrequent = mostFrequentClass
  
  	}
  	
  	/**
     * Returns the matching degree of the input example with the specified antecedents
     * @param membershipDegrees pre-computed membership degrees (the key of the first hash map is the variable index, and the one of the second hash map is the label index)
     * @param antecedents antecedents of the rule
     * @param example input example
     * @return matching degree of the input example with the specified antecedents
     */
    def computeMatchingDegree (membershipDegrees: Array[Array[Double]], antecedents: Array[Byte], example: Array[String]): Double = {
    	
    	var matching: Double = 1.0
    	var indexEx: Int = 0 
    	if(posClassLabels == 0){
    	  indexEx = 1
    	}
      // Compute matching degree - Without class labels to calculate
    	var i = 0
      while(i < (example.length - 2) && (matching > 0.0)){
      	// If it is a nominal value and it is not equal to the antecedent, then there is no matching
      	if (dataBase(i).isInstanceOf[FuzzyVariable]){
      		matching *= membershipDegrees(i)(antecedents(i)) //t-norma producto
      	}else {
      		if (!(dataBase(i).asInstanceOf[NominalVariable]).
      				getNominalValue(antecedents(i)).contentEquals(example(indexEx))){
      		  return 0.0
      		}
      	}
      	indexEx = indexEx + 1
      	i = i + 1
      }
    	
      matching
    }
      
    /**
     * Computes the membership degree of the input value to the specified fuzzy set
     * @param variable variable index
     * @param label linguistic label index
     * @param value input value
     * @return membership degree of the input value to the specified fuzzy set
     */
    def computeMembershipDegree (variable: Byte, label: Byte, value: String): Double = {
    	
    	if (dataBase(variable).isInstanceOf[NominalVariable]){
    		if (!(dataBase(variable).asInstanceOf[NominalVariable]).
    				getNominalValue(label).contentEquals(value)){
    			return 0.0
    		}else{
    			return 1.0
    		}
    	}

    	(dataBase(variable).asInstanceOf[FuzzyVariable]).getFuzzySets()(label).getMembershipDegree(value.toDouble)
    	
    }
    
    /**
  	 * Returns the fuzzy/nominal variable for i-th attribute 
  	 * @param i attribute id
  	 * @return fuzzy/nominal variable for i-th attribute
  	 */
  	def get (i: Int): Variable = dataBase(i)
  	
    /**
  	 * Returns the complete DataBase
  	 * @return the complete database (fuzzy and/or nominal variables)
  	 */
  	def getDataBase(): Array[Variable] = dataBase
  	
    /**
     * Returns class index
     * @return class index
     */
    def getClassIndex (): Array[Byte] ={
    	var classIndex = Array[Byte]()
    	for (index <- 0 to (classLabels.length - 1))
    			classIndex = classIndex :+ index.toByte
    	classIndex
    }
    
    /**
     * Returns class index
     * @param classLabel class label
     * @return class index
     */
    def getClassIndex (classLabel: String): Byte ={
    	var classIndex: Byte = -1
    	for (index <- 0 to (classLabels.length - 1))
    		if (classLabels(index).contentEquals(classLabel)){
    			classIndex = index.toByte
    		}
    	classIndex
    }
    
    /**
  	 * Returns class label
  	 * @param classIndex class index
  	 * @return class label
  	 */
  	def getClassLabel(classIndex: Byte): String = classLabels(classIndex)
  
  	/**
  	 * Returns class labels
  	 * @class labels
  	 */
  	def getClassLabels (): Array[String] = classLabels
  
  	/**
  	 * Returns the number of examples of each class
  	 * @return number of examples of each class
  	 */
  	def getClassNumExamples (): Array[Long] = classNumExamples
  
  	/**
  	 * Returns the number of examples of the class
  	 * @param classIndex index of the class
  	 * @return number of examples of the class
  	 */
  	def getClassNumExamples (classIndex: Byte): Long = classNumExamples(classIndex)
  	
  	/**
  	 * Returns the index of the most frequent class
  	 * @return index of the most frequent class
  	 */
  	def getMostFrequentClass (): Byte = classMostFrequent
  	
  	/**
  	 * Returns the number of classes
  	 * @return number of classes
  	 */ 
  	def getNumClasses (): Byte = {
  	  var result: Byte = -1
  		if (classLabels == null)
  			result = 0
  		if (classLabels.length<128)
  			result = classLabels.length.toByte
  		else{
  		  throw new 
  		      IllegalArgumentException("\nTHE NUMBER OF CLASS LABELS ("+classLabels.length+") EXCEEDS THE LIMIT (127)\n")
  			System.exit(-1)
  		}
  	  result
  	}
  	
  	/**
  	 * Returns the number of linguistic labels
  	 * @return number of linguistic labels
  	 */
  	def getNumLinguisticLabels(): Byte = numLinguisticLabels
  	
  	/**
  	 * Returns the number of variables
  	 * @return number of variables
  	 */
  	def getNumVariables (): Byte ={
  	  var result: Byte = 0
  		if (dataBase != null){
  			result = dataBase.length.toByte
  		}
  	  
  		result
  	}
  	
  	/**
     * Returns the position of the class labels in the datasets
     */
  	def getPosClassLabels(): Byte = posClassLabels
  	
  	/**
     * Returns a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     * @param inputValues input string representing the example
     * @return a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     */
    def getRuleFromExample (inputValues: Array[String]): Array[Byte] = {

        //var variables = Mediator.getVariables() // Only Features
        var labels = Array.fill(dataBase.length)(0.toByte)

        // Get each attribute label
        for (i <- 0 to (dataBase.length - 1)){
        	labels(i) = dataBase(i).getLabelIndex(inputValues(i))
        	if (labels(i) == -1)
  				  throw new SecurityException("\nERROR RULES GENERATION: The value is not correct, this algorithm is not preparated for empty or N/A data, error="+inputValues(i)+"*\n")
        }

        labels
        
    }
     	
  	/**
  	 * It add a new Variable to the database for i-th attribute 
  	 * @param i id of the attribute
  	 * @param var nominal or fuzzy variable
  	 */
  	def set(i: Int, variable: Variable){
  		dataBase(i) = variable
  	}
  	
  	/**
  	 * It sets the class labels of the variables
  	 * @param classLabels the class labels
  	 */
  	def setClassLabels(classLabels: Array[String]){
  		this.classLabels = classLabels
  	}
  	
  	def setClassNumExamples(classNumExamples: Array[Long]){
  		this.classNumExamples = classNumExamples
  	}
  	
  	def setMostFrequentClass(clas: Byte){
  		this.classMostFrequent = clas
  	}
  	
  	def setNumClasses(num: Byte){
  		this.numClassLabels = num
  	}
  	
  	def setNumLinguisticLabels(num: Byte){
  		this.numLinguisticLabels = num
  	}
  	
  	/**
  	 * It carries out a two-tuples lateral displacement on the corresponding Fuzzy-Variables object (DB)
  	 * @param c Chromosome representation
  	 * @param bd id of the DB to be applied
  	 */
  	/*def twotuples (c: Chromosome){
  		for (i <- 0 to (dataBase.length - 1)) {
  			if (!(dataBase(i).isInstanceOf[NominalVariable])) {
  				(dataBase(i).asInstanceOf[FuzzyVariable]).twoTuples(c)
  			}
  		}
  	}*/
  	
  	/**
  	 * It prints the database into an string
  	 */
  	override def toString(): String = {
  		var output: String = ""
  		for (i <- 0 to (dataBase.length - 1)){
  			output += dataBase(i).toString()
  		}
  		output
  	}
  	
  	override def write(out: DataOutput) /*throws IOException*/{
  		var nVariables = getNumVariables()
  		out.writeInt(nVariables)
  		for (i <- 0 to (nVariables - 1)){
  			if(dataBase(i).isInstanceOf[NominalVariable]){
  				out.writeBoolean(true)
  			}else{
  				out.writeBoolean(false)
  			}
  			dataBase(i).write(out)
  		}
  		out.writeInt(classLabels.length)
  		for (j <- 0 to (classLabels.length - 1)){
  			out.writeUTF(classLabels(j))
  		}
  		out.writeInt(classNumExamples.length)
  		for (j <- 0 to (classNumExamples.length - 1)){
  			out.writeLong(classNumExamples(j))
  		}
  		out.writeByte(classMostFrequent)
  		out.writeByte(numClassLabels)
  	}
  
  	override def readFields(in: DataInput) {
  	  var dbSize = in.readInt()
  		dataBase = new Array[Variable](dbSize)
  		for (i <- 0 to (dbSize - 1)){
  			var nominal: Boolean = in.readBoolean()
  			var name: String = in.readUTF()
  			if (nominal){
  				dataBase(i) = new NominalVariable(name)
  				dataBase(i).readFields(in)
  			}
  			else{
  				dataBase(i) = new FuzzyVariable(name)				
  				dataBase(i).readFields(in)
  			}
  		}
  		var n = in.readInt()
  		classLabels = new Array[String](n)
  		for (j <- 0 to (classLabels.length - 1)){
  			classLabels(j)= in.readUTF()
  		}
  		n = in.readInt() 
  
  		classNumExamples = new Array[Long](n)
  		for (j <- 0 to (classNumExamples.length - 1)){
  			classNumExamples(j) = in.readLong()
  		}
  		classMostFrequent = in.readByte()
  		numClassLabels = in.readByte()
  	}
}