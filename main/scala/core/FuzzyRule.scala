package core

import org.apache.hadoop.io.Writable

import java.io.{DataInput, DataOutput}

import utils.ConsequentPart

/**
 * Represents a fuzzy rule 
 * @author Eva M. Almansa
 * @version 1.0
 */
class FuzzyRule extends Writable with Serializable {
    
  /**
   * Index of each antecedent (linguistic label index in case of fuzzy variable and nominal value index in case of nominal variable)
   */
   private var antecedents: Array[Byte] = null
  
  /**
   * For each row: Sum of Rule weight (0) and Counter of occurrences of equal classIndex (1).
   */
  private var ruleWeight_counter: Array[Array[Double]] = null
  
  /**
   * Class index
   */
  private var classIndex: Byte = -1
  
  /**
   * Creates a new fuzzy rule from an array of antecedents
   * @param antecedents antecedents of the rule
   * @param numClassLabels number of class labels
   */
  def this (antecedents: Array[Byte], numClassLabels: Byte){
     
     this()
  	 this.antecedents = antecedents.clone()
  	 this.ruleWeight_counter = Array.fill(numClassLabels, numClassLabels)(0.0) //For each class->(0=ruleWeight, 1=Counter)
  	 
  }
  
  /**
   * Creates a new fuzzy rule from an array of antecedents
   * @param antecedents antecedents of the rule
   * @param classIndex index of the class
   * @param numClassLabels number of class labels
   */
  def this (antecedents: Array[Byte], classIndex: Byte, numClassLabels: Byte){
     
     this()
  	 this.antecedents = antecedents.clone()
  	 this.classIndex = classIndex
  	 this.ruleWeight_counter = Array.fill(numClassLabels, numClassLabels)(0.0) //For each class->(0=ruleWeight, 1=Counter)
  	 
  }
   
  /**
   * Creates a new fuzzy rule from an array of antecedents, a class index, and a rule weight
   * @param antecedents antecedents of the rule
   * @param classIndex class index of the rule
   * @param ruleWeight rule weight
   */
  def this (antecedents: Array[Byte], classIndex: Byte, ruleWeight: Double, numClassLabels: Byte){
     
     this()
  	 this.antecedents = antecedents.clone()
  	 this.classIndex = classIndex
  	 this.ruleWeight_counter = Array.fill(numClassLabels, 2)(0.0) //For each class->(0=ruleWeight, 1=Counter)
  	 this.ruleWeight_counter(classIndex)(0) = ruleWeight
  	 this.ruleWeight_counter(classIndex)(1) = 1.0
  	 
  }
   
  private def updateClassIndexOld(){
    if(ruleWeight_counter(0)(1) > 0.0 && ruleWeight_counter(1)(1) < 1.0)
      classIndex = 0
    else if (ruleWeight_counter(0)(1) < 1.0 && ruleWeight_counter(1)(1) > 0.0)
      classIndex = 1
    else if ((ruleWeight_counter(0)(0)/ruleWeight_counter(0)(1)) > (ruleWeight_counter(1)(0)/ruleWeight_counter(1)(1)))
      classIndex = 0
    else if ((ruleWeight_counter(0)(0)/ruleWeight_counter(0)(1)) == (ruleWeight_counter(1)(0)/ruleWeight_counter(1)(1))){
      if ((ruleWeight_counter(0)(0)) >= (ruleWeight_counter(1)(0)))
        classIndex = 0
      else 
        classIndex = 1
    }else
      classIndex = 1
  }
  
  private def updateClassIndex(){
    //In case of equal weights, positive class (0) has preference
    if(ruleWeight_counter(1)(0) > ruleWeight_counter(0)(0))
      classIndex = 1
    else 
      classIndex = 0
  }
  
  override def clone(): FuzzyRule = {
    
  	var fr = new FuzzyRule(antecedents, this.classIndex, ruleWeight_counter.length.toByte)
  	for(i <- 0 to (ruleWeight_counter.length - 1)){
  	  for(j <- 0 to (ruleWeight_counter(i).length - 1))
  	    fr.ruleWeight_counter(i)(j) = this.ruleWeight_counter(i)(j)
  	}
  	fr
  	
  }
  
  /**
   * Checks if the antecedents are equal and selects the FuzzyRule with the biggest rule weights. 
   */
  def equalsOld(obj:Any): Boolean = {

    if(obj.isInstanceOf[FuzzyRule] && (obj.asInstanceOf[FuzzyRule].antecedents sameElements this.antecedents)){
      /* MAX
       * if(this.getRuleWeight < obj.asInstanceOf[FuzzyRule].getRuleWeight){
        this.classIndex = obj.asInstanceOf[FuzzyRule].classIndex
        this.ruleWeight_counter(0)(0) = obj.asInstanceOf[FuzzyRule].ruleWeight_counter(0)(0)
        this.ruleWeight_counter(0)(1) = 1
        this.ruleWeight_counter(1)(0) = obj.asInstanceOf[FuzzyRule].ruleWeight_counter(1)(0)
        this.ruleWeight_counter(1)(1) = 1
      }*/
     
     if(classIndex != -1 && obj.asInstanceOf[FuzzyRule].classIndex != -1){
     
       	//println("@ Equals this= "+ antecedents.deep.mkString(" ") +" | Class = " + classIndex + " | Weight positive= " + ruleWeight_counter(0).deep.mkString(", ") + " | Weight negative= " + ruleWeight_counter(1).deep.mkString(", "))
        //println("@ Equals obj= "+ obj.asInstanceOf[FuzzyRule].antecedents.deep.mkString(" ") +" | Class = " + obj.asInstanceOf[FuzzyRule].classIndex + " | Weight positive= " + obj.asInstanceOf[FuzzyRule].ruleWeight_counter(0).deep.mkString(", ") + " | Weight negative= " + obj.asInstanceOf[FuzzyRule].ruleWeight_counter(1).deep.mkString(", "))
      
        //Rule Weight
        for(index <- 0 to (this.ruleWeight_counter.length - 1)){
          if(obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(0) > 0){
            this.ruleWeight_counter(index)(0) = this.ruleWeight_counter(index)(0) + obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(0)
            this.ruleWeight_counter(index)(1) = this.ruleWeight_counter(index)(1) + obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(1)
          }
        }
        
        updateClassIndex()
        
        //println("@ Before this= "+ antecedents.deep.mkString(" ") +" | Class = " + classIndex + " | Weight positive= " + ruleWeight_counter(0).deep.mkString(", ") + " | Weight negative= " + ruleWeight_counter(1).deep.mkString(", "))
      }else if (classIndex == -1 && obj.asInstanceOf[FuzzyRule].classIndex != -1){
          this.classIndex = obj.asInstanceOf[FuzzyRule].classIndex
          this.ruleWeight_counter = obj.asInstanceOf[FuzzyRule].ruleWeight_counter.clone
      }
      
      return true
    }
    return false
  }
  
  /**
   * Checks if the antecedents are equal (for hashmap) 
   */
  override def equals(obj:Any): Boolean = {

    if(obj.isInstanceOf[FuzzyRule] && (obj.asInstanceOf[FuzzyRule].antecedents sameElements this.antecedents)){
      /*
      var clas: Byte = obj.asInstanceOf[FuzzyRule].getClassIndex()
      if (clas >= 0){ //rule already created (not during learning stage)
        var rw: Double = obj.asInstanceOf[FuzzyRule].getRuleWeight()
        this.ruleWeight_counter(clas)(0) = ruleWeight_counter(clas)(0) + rw;
        this.ruleWeight_counter(clas)(1) = ruleWeight_counter(clas)(1) + 1;
        //println("Mira: "+ruleWeight_counter.deep.toString());
        //println("@ Before this= "+ antecedents.deep.mkString(" ") +" | Class = " + classIndex + " | Weight positive= " + ruleWeight_counter(0).deep.mkString(", ") + " | Weight negative= " + ruleWeight_counter(1).deep.mkString(", "))
      }
      */
      if (classIndex >= 0){ //rule already created (not during learning stage)
        for(index <- 0 to (this.ruleWeight_counter.length - 1)){
          obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(0) = obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(0) + this.ruleWeight_counter(index)(0);
          obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(1) = obj.asInstanceOf[FuzzyRule].ruleWeight_counter(index)(1) + this.ruleWeight_counter(index)(1);
        }
        //println("Mira: "+ruleWeight_counter.deep.toString());
        //println("@ Before this= "+ antecedents.deep.mkString(" ") +" | Class = " + classIndex + " | Weight positive= " + ruleWeight_counter(0).deep.mkString(", ") + " | Weight negative= " + ruleWeight_counter(1).deep.mkString(", "))
      }
      return true
    }
    return false
  }
  
  /**
   * It is necessary in the equals function. 
   */
  override def hashCode(): Int = {
  	new String(antecedents).hashCode()
  }
  
  /**
   * Returns the label index of the antecedent in the specified position
   * @param position position of the antecedent
   * @return label index of the antecedent in the specified position
   */
  def getAntecedent (position: Byte): Byte = {
      antecedents(position)
  }
    
  /**
   * Returns the antecedents 
   * @return the antecedents 
   */
  def getAntecedent (): Array[Byte] = {
      antecedents
  }
    
  /**
   * Returns the rule class index
   * @return rule class index
   */
  def getClassIndex (): Byte = {
      classIndex
  }
    
  /**
   * Returns the rule weight
   * @return rule weight
   */
  def getRuleWeightOld (): Double = {
    var ruleWeight = 0.0
    if(classIndex != -1){
      ruleWeight = ruleWeight_counter(classIndex)(0)/ruleWeight_counter(classIndex)(1)
    }
    ruleWeight
  }
  
  /**
   * Returns the rule weight, which must be previously well computed (normalized)
   * @return rule weight
   */
  def getRuleWeight (): Double = {
    var ruleWeight = 0.0
    if(classIndex != -1){
      ruleWeight = ruleWeight_counter(classIndex)(0)
    }
    ruleWeight
  }

  
  /**
   * Returns the rule weight
   * @param index index of the class
   * @return rule weight
   */
  def getRuleWeight (index: Byte): Double = ruleWeight_counter(index)(0)/ruleWeight_counter(index)(1)
  
  def getRuleWeightCounter (): Array[Array[Double]] = ruleWeight_counter
  
  /**
   * Sets the rule weight
   * @param rule weight
   * @param class index
   */
  def setRuleWeight (rw: Double, ci: Byte){
      this.ruleWeight_counter(ci)(0) = rw //Rule weight
      this.ruleWeight_counter(ci)(1) = 1.0 //Counter of each class label
  }
	
  def setRuleWeightCounter(rwCounter: Array[Array[Double]]){
    for(i <- 0 to (rwCounter.length - 1)){
      this.ruleWeight_counter(i)(0) = rwCounter(i)(0)  
      this.ruleWeight_counter(i)(1) = rwCounter(i)(1)
    }
    
    this.updateClassIndex()
  }
  
  def addRW(rw: Double, ci: Byte){
    ruleWeight_counter(ci)(0) = ruleWeight_counter(ci)(0) + rw;
    ruleWeight_counter(ci)(1) = ruleWeight_counter(ci)(1) + 1;
  }
  
  /**
   * Update the Consequent of the fuzzy rule: adapts the Rule Weight and the Class label
   * 
   * RW was accumulated during Reduce task, i.e. rules with same antecedent contribute to the RW in different maps.
   * Now, the RW is averaged and the final RW and class label is assigned as the maximum among the different classes 
   */
  def updateRWClass(){
    //extend for more than two classes
    //println("RW0 = "+ruleWeight_counter(0)(0)+", CL0= "+ruleWeight_counter(0)(1)+"; RW1= "+ruleWeight_counter(1)(0)+"; CL1: "+ruleWeight_counter(1)(1));
    if (ruleWeight_counter(0)(1) > 0)
      ruleWeight_counter(0)(0) = ruleWeight_counter(0)(0)/ruleWeight_counter(0)(1);
    if(ruleWeight_counter(1)(1) > 0)
      ruleWeight_counter(1)(0) = ruleWeight_counter(1)(0)/ruleWeight_counter(1)(1);
    updateClassIndex();
  }
  
  def toString (db: DataBase): String = {

      var output = "IF "
      
      for (i <- 0 to (antecedents.length - 2)){
          output += db.get(i).getName() + " IS "
          if (db.get(i).isInstanceOf[FuzzyVariable])
          	output += "L_" + antecedents(i) + " AND "
          else
          	output += (db.get(i).asInstanceOf[NominalVariable]).getNominalValue(antecedents(i)) + " AND "
      }
      output += db.get(antecedents.length-1).getName() + " IS "
      if (db.get(antecedents.length-1).isInstanceOf[FuzzyVariable])
      	output += "L_" + antecedents(antecedents.length-1)
      else
      	output += (db.get(antecedents.length-1).asInstanceOf[NominalVariable]).getNominalValue(antecedents(antecedents.length-1))
      
      output += " THEN CLASS = " + db.getClassLabel(classIndex) + " WITH RW = "+getRuleWeight
      
      output
      
  }

	override def readFields(in: DataInput) /*throws IOException*/ {
		var length = in.readInt()
		antecedents = new Array[Byte](length)

		for(j <- 0 to (length - 1))
			antecedents(j) = in.readByte()

		classIndex = in.readByte()
		ruleWeight_counter(classIndex)(0) = in.readFloat()
		ruleWeight_counter(classIndex)(1) = 1.0
	
	}
	
	override def write(out: DataOutput) /*throws IOException*/{
		var length: Int = 0

		if(antecedents != null)
			length = antecedents.length

		out.writeInt(length)

		for(j <- 0 to (length - 1))
			out.writeByte(antecedents(j))

		out.writeByte(classIndex)
		out.writeDouble(getRuleWeight)

  }
}