package utils

class CounterRules extends Serializable{
  
  private var negative: Int = 0
  private var positive: Int = 0
  
  def this(positive: Int, negative: Int){
    
    this()
    this.negative = negative
    this.positive = positive
    
  }
  
  def getNegative():Int = negative
  
  def getPositive():Int = positive
  
  override def clone (): CounterRules = {
    var cr = new CounterRules(positive, negative)
    cr
  }
  
  def reduceRules(counterRules: Array[Int]){
    positive = positive - counterRules(0)
    negative = negative - counterRules(1)
  }
}