package classifier

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import core.{DataBase, FuzzyRule, Mediator, KnowledgeBase}

/**
 * Launches MapReduce to classify input examples. 
 * @author Eva M. Almansa
 * @author Alberto Fernandez (alberto@decsai.ugr.es) - University of Granada
 * @version 1.0 (E. Almansa) - 05-feb-2017
 * @version 1.1 (A. Fernandez) - 12-jun-2017
 * 
 * Auxiliary class for storing the results of the classification (time and evaluation metrics)
 */
object ClassifierLauncher {

	/**
	 * Writes total execution time
	 */
	def writeExecutionTime(train: Boolean, startMs: Long, endMs: Long, sc: SparkContext) {

	  import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
	  import org.apache.hadoop.conf.Configuration
    import java.io.{BufferedWriter, BufferedReader, File, InputStreamReader, OutputStreamWriter, PrintWriter}
		
		var elapsed: Long = endMs - startMs
		var hours: Long = (elapsed / 3600000)
		var minutes: Long = (elapsed % 3600000) / 60000
		var seconds: Long = ((elapsed % 3600000) % 60000) / 1000

		try {
		  
		  var textPath:Path = null
		  var path_output = ""
  		if(!train)
  		  path_output = Mediator.getClassifierOutputPath()
  		else
  		  path_output = Mediator.getClassifierOutputPath()+"//Train"
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(new Path(path_output+"//TimeClassification000.txt"))){
		    textPath = new Path(path_output+"//TimeClassification000.txt")        	    
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(path_output+"//TimeClassification"+aux+i+".txt"))){
    			  textPath = new Path(path_output+"//TimeClassification"+aux+i+".txt")
    			  create = true
    		  }
		      i = i + 1
		    }
		  }
		  
      val bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath)))

			/**
			 * Write total execution time
			 */
			bwText.append("Total execution time in Classifier (hh:mm:ss):\n"
                                  +("00" + hours.toString()).substring(hours.toString().length())+":"
                                  +("00" + minutes.toString()).substring(minutes.toString().length())+":"
                                  +("00" + seconds.toString()).substring(seconds.toString().length())
                                  +" ("+(elapsed/1000).toString()+" seconds)\n")

			bwText.close()
  
		}catch{
		  case e: Exception => {
		    System.err.println("\nERROR WRITING TIME IN CLASSIFICATION")
			  e.printStackTrace()
			  System.err.println(-1)
		  }
		}

	}

	/**
	 * Writes the confusion matrix
	 * @param confusionMatrix input confusion matrix
	 * @throws IOException 
	 */
	def writeConfusionMatrix (train: Boolean, confusionMatrix: Array[Array[Int]], dataBase: DataBase, sc: SparkContext): String = /*throws IOException*/{
	  
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
    import java.io.{BufferedWriter, OutputStreamWriter}
	  
		var confMatrix: String = ""
		var path_output = ""
		if(!train)
		  path_output = Mediator.getClassifierOutputPath()
		else
		  path_output = Mediator.getClassifierOutputPath()+"//Train"
		
		try{
			var textPath:Path = null
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(new Path(path_output+"//ConfusionMatrix000.txt"))){
		    textPath = new Path(path_output+"//ConfusionMatrix000.txt")        	    
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(path_output+"//ConfusionMatrix"+aux+i+".txt"))){
    			  textPath = new Path(path_output+"//ConfusionMatrix"+aux+i+".txt")
    			  create = true
    		  }   
		      i = i + 1
		    }
		  }

			val bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath)))
			
			// Write table heading
			for (i <- 0 to (dataBase.getNumClasses() - 1)){
				bwText.write("\t"+"Prediction "+dataBase.getClassLabel(i.toByte))
				confMatrix += "\t"+dataBase.getClassLabel(i.toByte)
			}
			bwText.write("\n")
			confMatrix += "\n"

			// Write matrix content
			for (i <- 0 to (dataBase.getNumClasses() - 1)){		  
				bwText.write("Class "+dataBase.getClassLabel(i.toByte))
				confMatrix += dataBase.getClassLabel(i.toByte)
				for (j <- 0 to (dataBase.getNumClasses() - 1)){
					bwText.write("\t"+confusionMatrix(i)(j))
					confMatrix += "\t"+confusionMatrix(i)(j)
				}
				bwText.write("\n")
				confMatrix += "\n"
			}

			bwText.close()
			fs.close()
		}catch{
		  case e: Exception => {
	    System.err.println("\n ERROR WRITING CONFUSION MATRIX")
		  e.printStackTrace()
		  System.err.println(-1)
		 }
		}
		
		confMatrix
	}
	
	/**
	 * Writes the Area Under the ROC Curve (AUC) and if the file exists, will be accumulated.
	 * 
	 * It is also prepared to compute the geometric mean of the true rates (GM)
	 */
	def write_avgAUC_MG (train: Boolean, AUC: Double, last_iteration: Boolean, sc: SparkContext){
	  
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
    import java.io.{BufferedWriter, OutputStreamWriter}
		
	  var path_output = ""
		if(!train)
		  path_output = Mediator.getClassifierOutputPath()
		else
		  path_output = Mediator.getClassifierOutputPath()+"//Train"
		  
		try{
			var textPathTmp = new Path(path_output+"//AUC_Tmp.txt")
		  var bwText: BufferedWriter = null
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(textPathTmp)){    
		    bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPathTmp)))
		  }else {
		    bwText = new BufferedWriter(new OutputStreamWriter(fs.append(textPathTmp)))
		    bwText.write("\n")
		  }
		  
			//Write AUC
			bwText.write(AUC.toString)
			bwText.close()
			
			//Calculate the average of all iterations with AUC
			if(last_iteration){
			  val lines = sc.textFile(path_output+"//AUC_Tmp.txt", 1).collect()
			  var counter = 0
			  var sum, avg, MG: Double = 0.0 
			  
			  //Read all measure of AUC
			  for(line <- lines){
			    /*if(counter == 0)
			      MG = line.toDouble
			    else 
			      MG = MG * line.toDouble*/
			    sum = sum + line.toDouble
			    counter = counter + 1
			  }
			  
			  avg = sum/counter
			  
			  /*if(MG > 0)
			    MG = scala.math.pow(MG, 1.0/counter.toDouble)
			  else 
			    MG = 0.0*/
			    
			  fs.delete(textPathTmp,false)
			  
			  var textPath = new Path(path_output+"//avgAUC.txt")
			  bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath)))
			  bwText.write(" Number of iterations = " + counter + "\n Average AUC = "+avg.toString+"\n")
			  //bwText.write(" MG = "+MG.toString+"\n")
			  bwText.close()
			}

			fs.close()
		}catch{
		  case e: Exception => {
	    System.err.println("\n ERROR WRITING AUC TEMPORAL")
		  e.printStackTrace()
		  System.err.println(-1)
		 }
		}
	}

}
