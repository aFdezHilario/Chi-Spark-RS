package core

import java.io.{DataInput, DataOutput}

/**
 * Represents a fuzzy variable of the problem, containing <i>l</i> linguistic labels (fuzzy sets), being <i>l</i> the number of linguistic labels specified by the user
 * @author Eva M. Almansa
 * @version 1.0
 */
class FuzzyVariable extends Variable {
  
  /**
   * Fuzzy sets that compose this fuzzy variable
   */
  private var fuzzySets: Array[FuzzySet] = null
  
  /**
	 * Fuzzy sets that compose this fuzzy variable (copy for tuning)
	 */
	private var fuzzySetsIni: Array[FuzzySet] = null
  
  /**
   * Merge points of fuzzy sets
   */
  private var mergePoints: Array[Double] = null
  
  /**
   * Creates a new fuzzy variable
   * @param name variable name
   */
  def this(name: String){
  	
    this()
    this.Variable(name)
  }
  
  /**
   * Builds the fuzzy sets (modeled by triangular membership functions) that compose this fuzzy variable from the input limits 
   * @param lowerLimits lower limit of each variable
   * @param upperLimits upper limit of each variable
   * @param numLinguisticLabels number of linguistic labels (fuzzy sets) that compose this variable
   */
  def buildFuzzySets (lowerLimit: Double, upperLimit: Double, numLinguisticLabels: Byte){
      
      fuzzySets = new Array[FuzzySet](numLinguisticLabels)
      mergePoints = new Array[Double](numLinguisticLabels - 1)
      
      /***** Ruspini partitioning *****/
      
      for (label <- 0 to (numLinguisticLabels - 1)){
      
          // Compute the half of the triangle's base
          var halfBase = (upperLimit - lowerLimit) / (numLinguisticLabels - 1)
          
          // We add the half of the triangle's base n times to the lower limit,
          // depending on the linguistic label and the point we want to obtain (left, mid, right)
          var leftPoint = lowerLimit + halfBase * (label - 1)
          var midPoint = lowerLimit + halfBase * (label)
          var rightPoint = lowerLimit + halfBase * (label + 1)
          //println(" Left=" + leftPoint + ", mid=" + midPoint + ", rigth=" + rightPoint)
          if (label == 0){
              leftPoint = midPoint
          }else if (label == numLinguisticLabels - 1){
              rightPoint = midPoint
          }
          //println(" Left=" + leftPoint + ", mid=" + midPoint + ", rigth=" + rightPoint)
          // We create the fuzzy set
          fuzzySets(label) = new FuzzySet(leftPoint, midPoint, rightPoint, label.toByte)
          
          // We add the merge point
          if (label > 0){
              mergePoints(label-1) = midPoint - ((midPoint - fuzzySets(label-1).getMidPoint()) / 2)
          }   
      }
      
      fuzzySetsIni = new Array[FuzzySet](numLinguisticLabels)
  		for (i <- 0 to (fuzzySets.length - 1)){
  			fuzzySetsIni(i) = new FuzzySet(fuzzySets(i))
  		}
      
  }
  
  /**
	 * Carries out the two-tuples lateral displacement on this fuzzy variable (over all fuzzy sets)
	 * @param c chromosome codification
	 */
	/*def twoTuples(c: Chromosome){
		var pos: Int = 0
		var displacement: Double = 0.0
		for (label <- 0 to (fuzzySets.length - 1)){
			if (label == 0) displacement = (c.shift(pos) - 0.5) * (fuzzySetsIni(label+1).getMidPoint()  - fuzzySetsIni(label).getMidPoint())
			else if (label == (fuzzySets.length-1))  displacement = (c.shift(pos) - 0.5) * (fuzzySetsIni(label).getMidPoint() - fuzzySetsIni(label-1).getMidPoint())
			else {
				if ((c.shift(pos) - 0.5) < 0.0)  displacement = (c.shift(pos) - 0.5) * (fuzzySetsIni(label).getMidPoint() - fuzzySetsIni(label-1).getMidPoint())
				else  displacement = (c.shift(pos) - 0.5) * (fuzzySetsIni(label+1).getMidPoint() - fuzzySetsIni(label).getMidPoint())
			}

			fuzzySets(label).setLeftPoint(fuzzySetsIni(label).getLeftPoint() + displacement)
			fuzzySets(label).setMidPoint(fuzzySetsIni(label).getMidPoint() + displacement)
			fuzzySets(label).setRightPoint(fuzzySetsIni(label).getRightPoint() + displacement)
			if (label > 0)
				mergePoints(label-1) = fuzzySets(label).getMidPoint() - ((fuzzySets(label).getMidPoint() - fuzzySets(label-1).getMidPoint()) / 2.0)
			
			pos = pos + 1
		}
	}*/
	
  /**
   * Returns the fuzzy sets that compose this fuzzy variable
   * @return fuzzy sets that compose this fuzzy variable
   */
  def getFuzzySets(): Array[FuzzySet] ={
      fuzzySets
  }
    
  /**
   * Returns the variable label index corresponding to the input value
   * @param inputValue input value
   * @return Variable label index corresponding to the input value
   */
  override def getLabelIndex(inputValue: String): Byte = getMaxMembershipFuzzySet (inputValue.toDouble)
  
  def getMembershipDegree(label: Byte, value: Double): Double = fuzzySets(label).getMembershipDegree(value)
  
  override def clone(): FuzzyVariable = {
		var copy = new FuzzyVariable(this.getName())
		copy.fuzzySets = new Array[FuzzySet](this.fuzzySets.length)
		for (i <- 0 to (fuzzySets.length - 1)){
			copy.fuzzySets(i) = new FuzzySet(fuzzySets(i))
		}
		copy.fuzzySetsIni = this.fuzzySetsIni.clone()
		copy.mergePoints = this.mergePoints.clone()
		copy
	}
  
  /**
   * Returns the index of the fuzzy set with the highest membership degree for the input value
   * @param value input value
   * @return index of the fuzzy set that returns the highest membership degree for the input value
   */
  private def getMaxMembershipFuzzySet (value: Double): Byte ={
      
      var index: Byte = -1
      
      // Since this function is used only in the learning stage,
      // we do not compute membership degrees. Instead, we check
      // the location of the input value with respect to the point
      // where two fuzzy sets merge.
      var i: Byte = 0
      while((i < mergePoints.length) && (index == -1)){
          if (value < mergePoints(i)){
              index = i
          }
          i = i.+(1).toByte
      }
      if (index < 0){
          index = mergePoints.length.toByte
      }
      //println(" Value="+value+", index="+index+ ", mergePoint="+ mergePoints.mkString(" - "))
      index
  }
  
  override def toString(): String ={
        
        var output = getName() + ":\n"
        
        //output += " Fuzzy Sets => \n"
        for (i <- 0 to (fuzzySets.length - 1)){
            output += " L_"+i.toString()+": ("+
                      fuzzySets(i).getLeftPoint().toString()+","+
                      fuzzySets(i).getMidPoint().toString()+","+
                      fuzzySets(i).getRightPoint().toString()+")\n"
        }
        
        /*output += " Merge Points => \n"
        for(i <- 0 to (mergePoints.length - 1)){
          output += "Merge_"+i+": (" + mergePoints(i) + ") \n"
        }*/
        
        output
  }
  
  def getMergePoints(): Array[Double] = {
		this.mergePoints.clone()
	}

	override def write(out: DataOutput) /*throws IOException*/{
		out.writeUTF(this.getName())
		out.writeInt(this.fuzzySets.length)
		for (j <- 0 to (this.fuzzySets.length - 1)){
			fuzzySets(j).write(out)
		}
		out.writeInt(this.mergePoints.length)
		for (j <- 0 to (mergePoints.length - 1)){
			out.writeDouble(mergePoints(j))					
		}
	}

	override def readFields(in: DataInput) /*throws IOException*/ {
		var nFuzz = in.readInt()
		fuzzySets = new Array[FuzzySet](nFuzz)
		for (j <- 0 to (fuzzySets.length - 1)){
			var fs = new FuzzySet()
			fs.readFields(in)
			fuzzySets(j) = fs
		}
	  var nMP = in.readInt()
		mergePoints = new Array[Double](nMP)
		for (j <- 0 to (mergePoints.length - 1)){
			mergePoints(j) = in.readDouble()					
		}		
	}
}