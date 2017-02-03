package classifier

import org.apache.spark.AccumulatorParam

import core.{DataBase, Mediator}

/**
 * Reducer object used to sum received confusion matrices 2x2
 * @author Eva M. Almansa
 * @version 1.0
 */
object ConfusionMatrixReducer extends AccumulatorParam[Array[Array[Int]]] { 
  
  override def zero(initialValue: Array[Array[Int]]): Array[Array[Int]] = {
    Array.fill(initialValue.size, initialValue.size)(0)
  }

  override def addInPlace(s1: Array[Array[Int]], s2: Array[Array[Int]]): Array[Array[Int]] = {
      var solution: Array[Array[Int]] = Array.fill(s1.size, s1.size)(0)
 
      solution(0)(0) = s1(0)(0) + s2(0)(0) 
      solution(0)(1) = s1(0)(1) + s2(0)(1) 
      solution(1)(1) = s1(1)(1) + s2(1)(1)
      solution(1)(0) = s1(1)(0) + s2(1)(0)
       
      solution  
  }
	
  def metricsConfusionMatrix(solution: Array[Array[Int]], dataBase: DataBase): Double = {
    var exist, wrong, TP, TN, FP, FN: Int = 0
    for(classIndex <- 0 to (solution.size - 1)){
      if(classIndex == 0){
        //println("@ C=" + dataBase.getClassLabel(classIndex.toByte) + " | TP =" + solution(classIndex)(0) + " FP =" + solution(classIndex)(1))
        /*exist = exist + solution(classIndex)(0)
        wrong = wrong + solution(classIndex)(1)*/
        TP = TP + solution(classIndex)(0)
        FP = FP + solution(classIndex)(1)
      }
      else{
        //println("@ C=" + dataBase.getClassLabel(classIndex.toByte) + " | FN =" + solution(classIndex)(0) + " TN =" + solution(classIndex)(1))
        /*wrong = wrong + solution(classIndex)(0)
        exist = exist + solution(classIndex)(1)*/
        FN = FN + solution(classIndex)(0)
        TN = TN + solution(classIndex)(1)
      }
    }
    //TPR(𝑟𝑒𝑐𝑎𝑙𝑙/𝑠𝑒𝑛𝑠𝑖𝑣𝑖𝑡𝑦), TNR(𝑠𝑝𝑒𝑐𝑖𝑓𝑖𝑡𝑦=1−𝐹𝑃𝑅)
    val TPR = (TP/(TP+FN).toDouble)
    val FPR = (FP/(FP+TN).toDouble) //FP / (FP + TN)
    val AUC = ((1 + TPR - FPR) / 2.0)
    /*println("@ Total examples = " + (exist+wrong))
    println("@ Total exists = " + exist + ", total wrong = " + wrong)
    println("@ Accuracy => " + (exist/(exist+wrong).toDouble))
    
    println("@ TPR => " + TPR)
    println("@ FPR => " + FPR)
    //println("@ TNR => " + (TN/(TN+FP).toDouble))
    println("@ AUC => " + AUC)*/

    AUC
  }
}

