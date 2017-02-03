package learner

import core.{FuzzyRule, KnowledgeBase, Population}

/**
 * Reducer class used to gather rules
 * @author Eva M. Almansa
 * @version 1.0
 */
object RulesGenerationReducer extends Serializable {
  
  def reduce(op1: KnowledgeBase, op2: KnowledgeBase): KnowledgeBase = {
   
      var merge = Set[FuzzyRule]()
      /**
    	 * Compute the rule weight of each rule and solve the conflicts with function in ByteArrayWritable
    	 */
    	try{

    	  merge ++= op1.getRuleBase().toSet
    	  merge ++= op2.getRuleBase().toSet
    	  
    	}catch {
    	  case e: Exception => {
          System.err.println("ERROR REDUCE PARTITION: \n")
          e.printStackTrace()
          System.exit(-1)}
    	}
    	
    	var result = merge.toArray
    	
    	//*TMP
    	var kb = new KnowledgeBase(op1.getDataBase(), result)
    	if(op1.getCounterRules() == null)
    	  kb.addCounterRules(op1.getCounter())
    	else 
    	  kb.addCounterRules(op1.getCounterRules())
    	if(op2.getCounterRules() == null)
    	  kb.addCounterRules(op2.getCounter())
    	else 
    	  kb.addCounterRules(op2.getCounter())
    	kb
    	
    	/*TMP
    	var cc = Array[Array[Int]]()
    	cc = op1.getCounterClass()
    	for(i <- 0 to (op2.getCounterClass().length - 1))
    	  cc = cc :+ op2.getCounterClass()(i)
    	
    	new KnowledgeBase(op1.getDataBase(), result, cc)*/
    	
    	
    	//new KnowledgeBase(op1.getDataBase(), result)
  }
 
}
