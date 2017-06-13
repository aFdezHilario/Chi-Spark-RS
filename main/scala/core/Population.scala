package core

import org.apache.spark.SparkContext

import utils.Randomize

/**
 * Population: the main procedure for the evolutionary optimization
 * 
 * @author Eva Almansa (eva.m.almansa@gmail.com)
 * @author Alberto Fernandez (alberto@decsai.ugr.es) - University of Granada
 * @version 1.0 (E. Almansa) - 05-feb-2017
 * @version 1.1 (A. Fernandez) - 12-jun-2017
 * 
 * It contains a CHC EA for optimizing an imbalanced classification problem with rule selection 
 */
class Population extends Serializable{
  
	var population: Array[Chromosome] = null
	var kb: KnowledgeBase = null
	var nEvals, totalEvals, bitsGen, popSize, cromLength: Int = 0
	var crossProb, threshold, bestFitness, alpha: Double = 0.0
	var nuevos: Boolean = false
	var inputValues: Array[Array[String]] = null
	var classLabels: Array[Byte] = null
	
	/**
	 * Constructor with parameters
	 * @param kb Knowledge Base
	 * @param popSize size of the population
	 * @param nEvals number of evaluations
	 * @param crossProb crossover probability
	 * @param bitsGen bits per gen (for gray codification: incest prevention)
	 * @param values examples of the dataset
	 * @param seed seed of initialization random
	 */
	def this (kb: KnowledgeBase, popSize: Int, nEvals: Int, crossProb: Double, bitsGen: Int, alpha: Double, values: Array[Array[String]], classLabels: Array[Byte]){
	  
	  this()
		this.kb = kb.clone()
		this.nEvals = nEvals
		this.totalEvals = nEvals
		this.crossProb = crossProb
		this.bitsGen = bitsGen
		this.popSize = popSize
		this.population = Array[Chromosome]()
		this.cromLength = kb.getSizeRuleBase()
		this.threshold = cromLength*bitsGen/4.0
		this.bestFitness = 0
		this.alpha = alpha
		this.inputValues = Array[Array[String]]()
		for (i <- 0 to (values.length - 1)){
			this.inputValues = this.inputValues :+ new Array[String](values(i).length - 1)
			for (j <- 0 to (values(i).length - 2)){
				this.inputValues(i)(j) = values(i)(j)
			}
		}
		this.classLabels = classLabels
	}
	
	/**
   * Maximization
   * @param a double first number
   * @param b double second number
   * @return boolean true if a is better than b
   */
  def BETTER(a: Double, b: Double): Boolean = {
    if (a > b) {
      return true
    }
    return false
  }
	
	/**
	 * It sets the initial population.
	 * 
	 * First chromosome has all bits active (standard DB). The remaining ones are set up randomly
	 */
	private def Initialize(){
		var c = new Chromosome(cromLength,1)
		population = population :+ (c)
		for (i <- 1 to (popSize - 1)){
			c = new Chromosome(cromLength)
			population = population :+ c
		}
	}
	
	/**
	 * It sets the initial population.
	 * 
	 * First chromosome has all bits active (standard DB). The remaining ones are set up randomly
	 */
	private def Initialize(ini: Chromosome){
		population = population :+ ini
		for (i <- 1 to (popSize - 1)){
			var c = new Chromosome(cromLength) 
			population = population :+ c
		}
	}
	
	/**
	 * Standard fitness (classifier accuracy)
	 */
	private def classifyAcc(individual: Array[Byte]): Double = fitnessAccuracy(individual)
	
	/**
	 * It computes the accuracy of the classifier. Only selected rules are used (individual)
	 */
	private def fitnessAccuracy(individual: Array[Byte]): Double = {
	  var hits: Int = 0
		for (i <- 0 to (this.inputValues.length - 1)){
			var classIndex: Byte = kb.classifyRS(Mediator.getFRM(), individual, inputValues(i))
			if (classIndex == this.classLabels(i)){
				hits = hits + 1
			}
		}	  
	  (1.0*hits/this.inputValues.length.toDouble)
	}
	
	/**
	 * Weighted fitness: Rule Selection + AUC
	 */
	private def classify(individual: Array[Byte]): Double = fitnessAUC(individual)*alpha + (((this.cromLength-selectionRules(individual))/this.cromLength)*(1.0-alpha))
	
	/**
	 * It computes the number of selected rules (1s in the chromosome)
	 */
	private def selectionRules(individual: Array[Byte]): Double = {
	  var counter = 0.0
	  for (ind <- individual){
	    if(ind == 1)
	      counter = counter + 1.0
	  }
	  //uncomment for debug
	  //println("@ Selection= " + counter.toString)
	  counter
	}
	
	/**
	 * It computes the AUC of the classifier.
	 */
	private def fitnessAUC(individual: Array[Byte]): Double = {
	  var TP, TN, FP, FN: Int = 0
		for (i <- 0 to (this.inputValues.length - 1)){
			var classIndex: Byte = kb.classifyRS(Mediator.getFRM(), individual, inputValues(i))
			if (this.classLabels(i) == 0){ //0 stands for positive class
			  if(classIndex == this.classLabels(i))
				  TP = TP + 1
			  else
          FN = FN + 1
			}
			else{
			  if(classIndex == this.classLabels(i))
				  TN = TN + 1
			  else
          FP = FP + 1
			}
		}
	  
	  var TPR, FPR: Double = 0.0
	  if((TP+FN).toDouble > 0)
	    TPR = (TP.toDouble/(TP+FN).toDouble)
    if((FP+TN).toDouble > 0)
      FPR = (FP.toDouble/(FP+TN).toDouble) //FP / (FP + TN)
    val AUC =  ((1.0 + TPR - FPR) / 2.0)
    //Uncomment for debug
    /*
    System.err.println("@KB: "+kb.counterClassLabels().deep.mkString(" "));
    System.err.println((this.inputValues.length - 1)+" TP: "+TP+"; FP: "+FP+"; TN: "+TN+"; FN: "+FN+" @ AUC=" + AUC.toString)
    */
	  AUC
	}
	
	
	/**
	 * Evaluation procedure of the chromosomes
	 */
	private def Evaluate(sc: SparkContext): Double = {
		var best: Double = 0.0
		population.foreach { c => 
		  if (!c.isEvaluated()){
				nuevos = true //There is at least one new chromosome in the population
				var acc: Double = classify(c.getIndividual())
				//uncomment for debug
				//println("@ AUC fitness= "+acc.toString)
				c.setFitness(acc)
				c.evaluated()
				nEvals = nEvals - 1
				if (acc > best)
					best = acc
			} 
		}
		
		best
	}
	
	/**
	 * Crossover function (HUX)
	 */
	def Cross(){
	  
		//Random sort of the population
		var sample = Array.range(0, population.length) 
		val max = sample.length - 1
		val min = 1
		for (i <- 0 to (sample.length - 1)){
			var j = Randomize.getRandom.nextInt(max - min + 1) + min //Including both: min and max
			var temp = sample(j)
			sample(j) = sample(i)
			sample(i) = temp
		}
		
		//Select two parents
		for (i <- 0 to (sample.length - 2) by 2){
			var mom = population(sample(i))
			var dad = population(sample(i+1))
			HUX(mom,dad)
		}
	}
	
	/**
	 * HUX crossover procedure. Half of the different genes are crossed.
	 */
	private def HUX(mom: Chromosome, dad: Chromosome){
    var son1 = mom.clone()
    var son2 = dad.clone()
    var posiciones = son1.differ(son2)
    var distintos = son1.nDifferents()
    
    var intercambios = distintos / 2
    if ((distintos > 0) && (intercambios == 0)) 
      intercambios = 1
    
    var flips = new Array[Int](intercambios)
    for (j <- 0 to (intercambios - 1)) {
      distintos = distintos - 1
      if(distintos >= 0)
        flips(j) = posiciones(Randomize.getRandom.nextInt(distintos + 1))
    }
    son1.flip(flips)
    son2.flip(flips)
    
    //Insert
    population = population :+ (son1)
    population = population :+ (son2)
  }
	
	/*private def xPC_BLX(mom: Chromosome, dad: Chromosome){
		var son1 = mom.clone()
		var son2 = dad.clone()
		son1.xPC_BLX(mom, dad, random)
		son2.xPC_BLX(dad, mom, random)
		//Insert
		population = population :+ son1
		population = population :+ son2
	}*/
	
	private def Select(): Double = {
		population = population.sortWith(_.compareTo(_) < 0)
		//rb.Generation(population.get(0))
		var bestFitness = population(0).getFitness()
		population = population.slice(0, popSize) //Sub-array [0-popSize]
		bestFitness
	}

	private def Restart(){
		population = population.sortWith(_.compareTo(_) < 0)
		var best = population(0)
		population = Array[Chromosome]()//remove all population
		Initialize(best)
	}

	/**
	 * It launches the evolutionary process
	 */
	def Generation(sc: SparkContext): KnowledgeBase = {
		var resets = 0
		
		Initialize()
		//uncomment for debug
		//println("@ Initilization complete...")		
		this.bestFitness = Evaluate(sc)
		var bestFitness = this.bestFitness 
		println("@ Evaluations remaining = "+nEvals+", AUC init = "+bestFitness+", resets = "+ resets)
		while((nEvals > 0)&&(bestFitness < 1.0)&&(resets < 3)){
			Cross()
			nuevos = false
			bestFitness = Evaluate(sc)
			Select()
			//uncomment for debug
			//println("@ Check Evaluations = "+nEvals+", AUC found = "+bestFitness+", resets = "+ resets)
			if (bestFitness > this.bestFitness){
				this.bestFitness = bestFitness
				resets = 0
				println("@ Evaluations remaining = "+nEvals+", Best AUC = "+this.bestFitness+".")
			}
			if (!nuevos){ //No new chromosomes in the population
				threshold = threshold - bitsGen
				if (threshold < 0){
					println("*** Restarting ***")
					Restart()
					threshold = (cromLength*bitsGen)/4.0
          resets = resets + 1
          bestFitness = Evaluate(sc)
          //uncomment for debug
          //println("@ Check Evaluations = "+nEvals+", AUC found = "+bestFitness+", resets = "+ resets)
					if (bestFitness > this.bestFitness){
						this.bestFitness = bestFitness
						resets = 0
						println("@ Evaluations remaining = "+nEvals+", Best Accuracy = "+this.bestFitness+".")
					}     
				}
			}
		}
		
		this.best()
	}
	
		
	/**
	 * Outputs the best chromosome
	 * @return the best chromosome
	 */
	def best(): KnowledgeBase = {
	 
	  population = population.sortWith(_.compareTo(_) < 0)
		var c = population(0)
		kb.removeRules(c.getIndividual())
		
		return kb
	}
	
	override def toString(): String ={
	  
	  var output = "Population:\n"
	  output += " kb=" + kb.toString
		output += "\n nEvals=" + nEvals.toString
		output += " | totalEvals=" + totalEvals.toString
		output += " | crossProb=" + crossProb.toString
		output += " | bitsGen=" + bitsGen.toString
		output += " | popSize=" + popSize.toString
		output += "\n population=" + population.deep.mkString(" ").toString
		output += "\n cromLength=" + cromLength.toString
		output += " | threshold=" + threshold.toString
		output += " | bestFitness=" + bestFitness.toString
		output += "\n inputValues=" + inputValues.deep.mkString(" ").toString
		output += "\n classLabels=" + classLabels.deep.mkString(" ").toString
		output
	}

}
