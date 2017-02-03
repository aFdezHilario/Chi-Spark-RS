package core

import java.io.{DataInput, DataOutput, IOException}

import org.apache.hadoop.io.Writable

/**
 * Represents a fuzzy set
 * @author Eva M. Almansa
 * @version 1.0
 */
class FuzzySet extends Writable with Serializable {
  

 /**
 	* Left point of the triangle
 	*/
  private var leftPoint: Double = 0.0
  
  /**
   * Mid point of the triangle
   */
  private var midPoint: Double = 0.0
  
  /**
   * Right point of the triangle
   */
  private var rightPoint: Double = 0.0
  
  /**
   * Linguistic label associated with this fuzzy set
   */
  private var labelIndex: Byte = 0
  
  /**
   * Creates a fuzzy set modeled by a triangular membership function
   * @param leftPoint left point of the triangle
   * @param midPoint mid point of the triangle
   * @param rightPoint right point of the triangle
   * @param labelIndex index of the linguistic label associated with this fuzzy set
   */
  def this (leftPoint: Double, midPoint: Double, rightPoint: Double, labelIndex: Byte){
    
      this()
      this.leftPoint = leftPoint
      this.midPoint = midPoint
      this.rightPoint = rightPoint
      this.labelIndex = labelIndex
      
  }
  
  /**
   * Copy constructor
   * @param fs another Fuzzy Set
   */
  def this (fs: FuzzySet){
    
      this()
      this.leftPoint = fs.leftPoint
      this.midPoint = fs.midPoint
      this.rightPoint = fs.rightPoint
      this.labelIndex = fs.labelIndex
      
  }
  
  /**
   * Returns the linguistic label associated with this fuzzy set
   * @return linguistic label associated with this fuzzy set
   */
  def getLabelIndex(): Byte ={
      labelIndex
  }
  
  /**
   * Returns the left point of the triangle
   * @return left point of the triangle
   */
  def getLeftPoint(): Double ={
      leftPoint
  }
  
  /**
   * Returns the membership degree of the input value to this fuzzy set
   * @param value input value
   * @return membership degree of the input value to this fuzzy set
   */
  def getMembershipDegree (value: Double): Double ={
     
    if (leftPoint <= value && value <= midPoint){
        if (leftPoint == value && midPoint == value){
          return 1.0
        }    
        else{
           return ((value - leftPoint) / (midPoint - leftPoint))
        }
    }
    else if (midPoint <= value && value <= rightPoint){
        if (midPoint == value && rightPoint == value){
          return 1.0
        }
        else{
          return ((rightPoint - value) / (rightPoint - midPoint))
        }
    }
    
    return 0.0
  }
    
  /**
   * Returns the mid point of the triangle
   * @return mid point of the triangle
   */
  def getMidPoint(): Double ={
      midPoint
  }  
  
  /**
   * Returns the right point of the triangle
   * @return right point of the triangle
   */
  def getRightPoint(): Double ={
      rightPoint
  }
   
  /**
   * Sets the left point of the triangle
   * @param left point of the triangle
   */
  def setLeftPoint (value: Double){
      this.leftPoint = value
  }
  
  /**
   * Sets the mid point of the triangle
   * @param mid point of the triangle 
   */
  def setMidPoint (value: Double){
      midPoint = value
  }
  
  /**
   * Sets the right point of the triangle
   * @param right point of the triangle 
   */
  def setRightPoint (value: Double){
      rightPoint = value
  }
  
  override def write(out: DataOutput) /*throws IOException*/{
		out.writeDouble(leftPoint)
		out.writeDouble(midPoint)
		out.writeDouble(rightPoint)
		out.writeByte(labelIndex)					
  }

	override def readFields(in: DataInput) /*throws IOException*/ {
		leftPoint = in.readDouble()
		midPoint = in.readDouble()
		rightPoint  = in.readDouble()
		labelIndex = in.readByte()
	}
	
	override def toString(): String = {
	  var output = "FuzzySet"
	  output += "\nLeftPoint=" + leftPoint.toString
	  output += " | MidPoint=" + midPoint.toString
	  output += " | RightPoint=" + rightPoint.toString
	  output += " | LabelIndex=" + labelIndex.toString + "\n"
	  output
	}
}