package core

import java.io.{DataInput, DataOutput}

class NominalVariable extends Variable {
    
  /**
   * Nominal values of this variable (if it is a nominal variable)
   */
  private var nominalValues: Array[String] = null
  
  /**
   * Constructor. Creates a new nominal variable
   * @param name variable name
   */
  def this (name: String){
  	
    this()
  	this.Variable(name)
  	
  }
  
  override def clone(): NominalVariable = {
		var copy = new NominalVariable(this.getName())
		copy.nominalValues = this.getNominalValues().clone()
		copy
	}
  
  /**
   * Returns the position of the input nominal value
   * @param nominalValue input nominal value
   * @return position of the input nominal value
   */
  private def getIndexOfNominalValue (nominalValue: String): Byte = {
    
      var index: Byte = -1
      for (i <- 0 to (nominalValues.length - 1)){
      	if (nominalValues(i).contentEquals(nominalValue)){
      		index = i.toByte
      		return index
      	}
      }
      index
      
  }
  
  /**
   * Returns the variable label index corresponding to the input value
   * @param inputValue input value
   * @return Variable label index corresponding to the input value
   */
  override def getLabelIndex(inputValue: String): Byte = {
  	getIndexOfNominalValue (inputValue)
  }
  
  /**
   * Returns the nominal value at the specified position
   * @param index position of nominal value
   * @return nominal value at the specified position
   */
  def getNominalValue(index: Byte): String = {
      nominalValues(index)
  }
  
  /**
   * Returns the nominal values that compose this variable
   * @return nominal values that compose this variable
   */
  def getNominalValues(): Array[String] = {
      nominalValues
  }
  
  /**
   * Sets nominal values
   * @param nominalValues input nominal values
   */
  def setNominalValues (nominalValues: Array[String]){
      this.nominalValues = nominalValues
  }
  
  override def toString(): String = {
      
      var output = getName() + " (nominal variable):\nNominal values: "
      
      for (i <- 0 to (nominalValues.length-2))
          output += nominalValues(i)+", "
      output += nominalValues(nominalValues.length-1)+"\n"
      
      output     
  }
  
  override def readFields(in: DataInput) /*throws IOException*/ {
		var nNom = in.readInt()
		nominalValues = new Array[String](nNom)
		for (j <- 0 to (nominalValues.length - 1)){
			nominalValues(j) = in.readUTF()
		}
	}
  	
  override def write(out: DataOutput) /*throws IOException*/{
		out.writeUTF(this.getName())
		out.writeInt(nominalValues.length)
		for (nom <- nominalValues){
			out.writeUTF(nom)
		}
	}
  
}