package utils

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

/**
 * Represents the consequent part of the rule, including the class and the rule weight
 * @author Eva M. Almansa
 *
 */
class ConsequentPart extends Writable with Serializable {

	private var classIndex: Byte = 0
	private var ruleWeight: Double = 0.0
	
	/**
	 * Default constructor
	 */
	//def this () {}
	
	/**
	 * Constructs a new consequent
	 * @param classIndex index of the class of the rule
	 * @param ruleWeight weight of the rule
	 */
	def this (classIndex: Byte, ruleWeight: Double) {
	  this()
		this.classIndex = classIndex
		this.ruleWeight = ruleWeight
	}
	
	def addRuleWeight(weight: Double){
    this.ruleWeight = this.ruleWeight + weight
	}
		
	/**
	 * Returns the class index
	 * @return class index
	 */
	def getClassIndex(): Byte = {
		classIndex
	}
	
	/**
	 * Returns the rule weight
	 * @return rule weight
	 */
	def getRuleWeight(): Double = {
		ruleWeight
	}
	
	override def readFields(in: DataInput) /*throws IOException*/ {
    	
    classIndex = in.readByte()
    ruleWeight = in.readDouble()
      
  }
	
	override def write(out: DataOutput) /*throws IOException*/ {
    	
        out.writeByte(classIndex)
        out.writeDouble(ruleWeight)
        
   }
	
	override def toString(): String = {
	  var output = "ClassIndex= " + classIndex.toString
	  output += " | RuleWeight= " + ruleWeight.toString
	  output
	}
	
}
