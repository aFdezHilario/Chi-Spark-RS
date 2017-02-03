package core

import org.apache.spark.SparkContext

import org.apache.hadoop.io.Writable

import java.io.{DataInput, DataOutput, IOException}

trait Variable extends Writable with Serializable {
    
  /**
   * Variable name
   */
  private var name: String = null
  
  /**
   * Creates a new variable
   * @param name variable name
   */
  protected def Variable(name: String){
 
  	this.name = name
  }
  
  /**
   * Returns the variable label index corresponding to the input value
   * @param inputValue input value
   * @return Variable label index corresponding to the input value
   */
  def getLabelIndex(inputValue: String): Byte
  
  /**
   * Returns the variable name
   * @return variable name
   */
  def getName(): String = {
      name
  }
  
  /**
   * Abstract functions
   */
  //def clone(): Object 
  
  def readFields(in: DataInput) //throws Exception
  
  def write(out: DataOutput) //throw new IOException
  
}