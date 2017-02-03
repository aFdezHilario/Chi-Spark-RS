package utils

object Randomize extends Serializable {
  val random = scala.util.Random
  
  def setSeed(seed: Long){
    random.setSeed(seed)
  }
  
  def getRandom: scala.util.Random = random
}