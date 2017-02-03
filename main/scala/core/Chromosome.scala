package core

import utils.Randomize

/**
 * Represents a Chromosome
 * @author Eva M. Almansa
 * @version 1.0
 */
class Chromosome extends Comparable[Object] with Serializable{

  private val RANDOM_MAX = 100000000
  private val RANDOM_MIN = 0
  
	private var individual: Array[Byte] = null
	private var max, min, fitness: Double = 0.0 
	private var evaluate: Boolean = false
	private var differents: Int = 0
	private var bitsGene: Int = 62
	private var gray: Array[Char] = null
	
	/**
	 * Constructor with parameters
	 * @param size length of the chromosome
	 */
	def this(size: Int){
	  this()
		min = 0.0
		max = 1.0
		evaluate = false
		individual = Array.fill(size)(0.toByte)
		for (j <- 0 to (individual.length - 1)){
			individual(j) = if(Randomize.getRandom.nextDouble < 0.5) 1 else 0 //0 (inclusive) and 1 (exclusive)
		}
		//println("Initialize: "+individual.deep.mkString(" "))
		fitness = 0
	}
	
	/**
	 * Constructor with parameters
	 * @param size length of the chromosome
	 * @param value value to be set
	 */
	def this(size: Int, value: Byte){
	  this()
		evaluate = false
		individual = Array.fill(size)(value)
		fitness = 0
	}
	
	/**
	 * Constructor with parameters
	 * @param size length of the chromosome
	 * @param pos variable id to set 0.5 
	 */
	def this(size: Int, pos: Int){
	  this()
		evaluate = false
		individual = Array.fill(size)(1)
		fitness = 0
	}
	
	/**
	 * Constructor by one point crossover
	 * @param mom first individual
	 * @param dad second individual
	 * @param corte cut point
	 */
	def this(mom: Chromosome, dad: Chromosome, corte: Int){
	  this()
		evaluate = false
		individual = new Array[Byte](mom.size())
		for (j <- 0 to (corte - 1)){
			individual(j) = mom.useRule(j)
		}
		for (j <- corte to (individual.length - 1)){
			individual(j) = dad.useRule(j)
		}
		fitness = 0
	}
	
	def size(): Int = {
		individual.length
	}
	
	def rbSize(): Int = {
		var size: Int = 0
		for (i <- 0 to (individual.length - 1)){
			if (individual(i) == 1) size = size + 1
		}
		size
	}
	
	def isEvaluated(): Boolean = {
		evaluate
	}
	
	def evaluated(){
		evaluate = true
	}
	
	def useRule(pos: Int): Byte = {
		individual(pos)
		//return true
	}
	
	def setFitness(fitness: Double){
		this.fitness = fitness
	}
	
	def getFitness(): Double = fitness

	def getIndividual(): Array[Byte] = individual
	
	/*****************************************************************/
	/* Translations between fixed point ints and reflected Gray code */
	/*****************************************************************/	
	private def Gray(binChar: Array[Char], pos: Int){
	  var aux_pos = pos
	  var i = 0
		while (i < (binChar.length - 1)){
			var a: Char = binChar(i)
			var b: Char = binChar(i+1)
      if((a & ~ b) == 1 || (~ a & b) == 1){
        gray(aux_pos) = '1'
      }else{
      	gray(aux_pos) = '0'
      }
      i = i + 2
			aux_pos = aux_pos + 1
    }
	}
	
	/*************************************************************************/
	/* Translations between string representation and floating point vectors */
	/*************************************************************************/
	/*def StringRep(){
		gray = new Array[Char](individualR.length*bitsGene)
		var pos = 0
		for (i <- 0 to (individualR.length - 1)){
			var bin: String = java.lang.Long.toBinaryString(java.lang.Double.doubleToRawLongBits(individualR(i)))
			var binChar: Array[Char] = bin.toCharArray()
			Gray(binChar, pos)
			pos += bitsGene
		}
	}*/
	
	/*def gray_(pos: Int): Char = {
		this.gray(pos)
	}*/
	
	/*def shift(pos: Int): Double = {
		individualR(pos)
	}*/
	
	/*def xPC_BLX(mom: Chromosome, dad: Chromosome, random: scala.util.Random){
		for (i <- 0 to (individualR.length - 1)){
			var I: Double = Math.abs(mom.shift(i)-dad.shift(i))
			var A1: Double = mom.shift(i)-I; if (A1 < min) A1 = min
			var C1: Double = mom.shift(i)+I; if (C1 > max) C1 = max
			individualR(i) = A1 + ((random.nextInt(RANDOM_MAX.toInt - RANDOM_MIN + 1) + RANDOM_MIN)/RANDOM_MAX.toDouble)*(C1-A1)
		}
	}*/
	
	/*def hamming(c: Chromosome): Int = {
		var dist: Int = 0
		StringRep()
		c.StringRep()
		for (i<-0 to (gray.length - 1)) if (c.gray(i) != gray(i)) dist += 1
		dist
	}*/
	
	override def clone(): Chromosome = {
		var c = new Chromosome()
		c.individual = individual.clone()
		c.fitness = fitness
		c.evaluate = false
		c.min = min
		c.max = max
		c
	}
	
	def differ (c: Chromosome): Array[Int] = {
		var differ: Array[Int] = new Array[Int](individual.length)
		differents = 0
		for (i <- 0 to (individual.length - 1)){
			if (individual(i) != c.useRule(i)){
				differ(differents) = i
				differents += 1
			}
		}
		return differ
	}
	
	def nDifferents(): Int ={
		differents
	}
	
	def flip(positions: Array[Int]){
		for (i <- 0 to (positions.length - 1)){
			individual(positions(i)) = if(individual(positions(i)) == 1) 0 else 1
		}
	}
	
	/*def flipFS(positions: Array[Int]){
    for (i <- 0 to (positions.length - 1)){
      individualFS(positions(i)) = !individualFS(positions(i))
    }
  }*/
	
	/*def getWeights(): Array[Double]={
		individualR
	}*/
	
	/**
	 * Compares the fitness of two Chrmosomes for the ordering procedure
	 * @param a Object a Chromsome
	 * @return int -1 if the current Chrosome is worst than the one that is compared, 1 for the contrary case and 0
	 * if they are equal.
	 */
	def compareTo(a: Object): Int = {
		if ( (a.asInstanceOf[Chromosome]).fitness < fitness) {
			return -1
		}
		if ( (a.asInstanceOf[Chromosome]).fitness > fitness) {
			return 1
		}
		return 0
	}

	override def toString(): String ={
	  var output = "Chromosome\n"
	  output += " Min=" + min.toString
		output += "| Max=" + max.toString
		output += "| Fitness=" + fitness.toString
		output += "| Evaluate=" + evaluate.toString
		output += "\n Individual=" + individual.deep.mkString(" ").toString
		output += "\n"
	  output
	}
	
}
