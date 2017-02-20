package learner

import core.{FuzzyRule, KnowledgeBase, DataBase}
import utils.{ConsequentPart}

/**
 * Reducer class used to gather rules
 * @author Eva M. Almansa
 * @version 1.0
 */
object RulesGenerationReducer extends Serializable {
  
  def reduce(op1: KnowledgeBase, op2: KnowledgeBase): KnowledgeBase = {
   
      var merge = Map[FuzzyRule, Int]()
      
      /**
    	 * Compute the rule weight of each rule and solve the conflicts with function in ByteArrayWritable
    	 */
    	try{
    	  
    	  op1.getRuleBase() foreach { rule =>
    	    merge += (rule -> 1)
    	  }
    	  /*println("*****************************************************************")
    	  for (m <- op1.getRuleBase())
    	    println("@ Op1 =" + m.getAntecedent().deep.mkString(" ") + " | C " + m.getClassIndex() + " | W " + m.getRuleWeight()+ " | 2 W " + m.getRuleWeightCounter().deep)
    	  for (m <- merge)
    	    println("@ Merge1 =" + m._1.getAntecedent().deep.mkString(" ") + " | C " + m._1.getClassIndex() + " | W " + m._1.getRuleWeight() + " | 2 W " + m._1.getRuleWeightCounter().deep)*/
    	  
    	  op2.getRuleBase() foreach { rule =>
    	    merge += (rule -> 1)
    	  }
    	  
    	  /*for (m <- op2.getRuleBase())
    	    println("@ Op2 =" + m.getAntecedent().deep.mkString(" ") + " | C " + m.getClassIndex() + " | W " + m.getRuleWeight()+ " | 2 W " + m.getRuleWeightCounter().deep)
    	  for (m <- merge)
    	    println("@ Merge3 =" + m._1.getAntecedent().deep.mkString(" ") + " | C " + m._1.getClassIndex() + " | W " + m._1.getRuleWeight() + " | 2 W " + m._1.getRuleWeightCounter().deep)
    	  println("-----------------------------------------------------------------------")*/
    	}catch {
    	  case e: Exception => {
          System.err.println("ERROR REDUCE PARTITION: \n")
          e.printStackTrace()
          System.exit(-1)}
    	}
    	
    	var result = Array[FuzzyRule]()
    	merge foreach { rule =>
    	  result = result :+ rule._1
    	}
    	
    	/*for(rule <- result)
    	  println("@ Rule - " + rule.getAntecedent().deep.mkString(" ") + " | C=" + rule.getClassIndex() + " | W=" + rule.getRuleWeight() + " | W=" + rule.getRuleWeightCounter().deep.mkString(" "))*/
    	  
    	var kb = new KnowledgeBase(op2.getDataBase(), result)
    	if(op1.getCounterRules() == null)
    	  kb.addCounterRules(op1.getCounter())
    	else 
    	  kb.addCounterRules(op1.getCounterRules())
    	if(op2.getCounterRules() == null)
    	  kb.addCounterRules(op2.getCounter())
    	else 
    	  kb.addCounterRules(op2.getCounterRules())
    	kb
  }
}
