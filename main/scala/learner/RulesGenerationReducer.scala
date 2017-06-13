package learner

import core.{FuzzyRule, KnowledgeBase, DataBase}
import utils.{ConsequentPart}
import org.apache.log4j.{Level, Logger}

/**
 * Reducer class used to gather rules
 * @author Eva M. Almansa
 * @author Alberto Fernandez (alberto@decsai.ugr.es) - University of Granada
 * @version 1.0 (E. Almansa) - 05-feb-2017
 * @version 1.1 (A. Fernandez) - 12-jun-2017
 * 
 * It carries out an aggregation (by pairs) of the rules. Must be implemented as key-value (original hadoop code)
 */
object RulesGenerationReducer extends Serializable {

	def reduce(op1: KnowledgeBase, op2: KnowledgeBase): KnowledgeBase = {

			var logger = Logger.getLogger(this.getClass());
			var merge = Map[FuzzyRule, ConsequentPart]();

			/**
			 * Compute the rule weight of each rule and solve the conflicts
			 */
			try{
				//first RB is completely stored (first one or aggregated from other iterations)
				op1.getRuleBase() foreach { rule =>
				merge += (rule -> new ConsequentPart(rule.getClassIndex(),rule.getRuleWeight()))
				}
				//Uncomment for debug
				/*
    	  println("*****************************************************************")
    	  for (m <- op1.getRuleBase())
    	    println("@ Op1 =" + m.getAntecedent().deep.mkString(" ") + " | C " + m.getClassIndex() + " | W " + m.getRuleWeight()+ " | 2 W " + m.getRuleWeightCounter().deep)
    	  //for (m <- merge)
    	    //println("@ Merge1 =" + m._1.getAntecedent().deep.mkString(" ") + " | C " + m._1.getClassIndex() + " | W " + m._1.getRuleWeight() + " | 2 W " + m._1.getRuleWeightCounter().deep)
				 */    	  
				for (rule <- op2.getRuleBase()){
					val aux = merge.get(rule)
							//println("@ Op2 =" + rule.getAntecedent().deep.mkString(" ") + " | C " + rule.getClassIndex() + " | W " + rule.getRuleWeight()+ " | 2 W " + rule.getRuleWeightCounter().deep)
							if( aux == None){ //fuzzy rule not in merge
								merge += (rule -> new ConsequentPart(rule.getClassIndex(),rule.getRuleWeight()))
							}else{
								var consequent = aux.get
										var consequent2 = new ConsequentPart(rule.getClassIndex(), rule.getRuleWeight())
										var class1 = consequent.getClassIndex()
										var class2 = consequent2.getClassIndex()
										var weight1 = consequent.getRuleWeight()
										var weight2 = consequent2.getRuleWeight()
										merge -= rule;
								if (class1 == class2){ //same class: average RW
									var weight = (weight1 + weight2) / 2.0 //must be replaced for other function.
											merge += (rule -> new ConsequentPart(class1, weight))
								}else{
									if (weight1 > weight2){
										merge += (rule -> new ConsequentPart(class1, weight1))
									}else{
										merge += (rule -> new ConsequentPart(class2, weight2))
									}
								}
							}
				}
				//Uncomment for debug
				/*
    	  for (m <- merge)
    	    println("@ Merge3 =" + m._1.deep.mkString(" ") + " | C " + m._2.getClassIndex() + " | W " + m._2.getRuleWeight())
    	  println("-----------------------------------------------------------------------")
				 */
			}catch {
			case e: Exception => {
				System.err.println("ERROR REDUCE PARTITION: \n")
				e.printStackTrace()
				System.exit(-1)}
			}

			//Store all rules as Array Fuzzy Rule. Previously, a Map structure was used for double consequent rules
			var result = Array[FuzzyRule]()
					merge foreach { rule =>
					result = result :+ new FuzzyRule (rule._1.getAntecedent(), rule._2.getClassIndex(), rule._2.getRuleWeight(), op1.getDataBase().getNumClasses())
			}

			//uncomment for debug
			//for(rule <- result)
			//logger.info("@ Rule - " + rule.getAntecedent().deep.mkString(" ") + " | C=" + rule.getClassIndex() + " | W=" + rule.getRuleWeight() + " | W=" + rule.getRuleWeightCounter().deep.mkString(" "))

			//Information about type of rules in RB / KB
			var kb = new KnowledgeBase(op2.getDataBase(), result);
			if(op1.getCounterRules() == null)
				kb.addCounterRules(op1.getCounter());
			else 
				kb.addCounterRules(op1.getCounterRules());
			if(op2.getCounterRules() == null)
				kb.addCounterRules(op2.getCounter());
			else 
				kb.addCounterRules(op2.getCounterRules());

			kb
	}
}
