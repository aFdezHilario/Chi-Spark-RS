package learner

import org.apache.spark.{SparkConf, SparkContext}

import core.{Mediator, FuzzyRule, KnowledgeBase, NominalVariable, Population}

object LearnerLauncher {
    
  private final val VALID_OPTIONAL_ARGS: Array[Char] = Array('p','v')
	private final val MAX_OPTIONAL_ARGS: Int = VALID_OPTIONAL_ARGS.length

	/**
	 * Writes the database in a file (including class labels)
	 * @throws IOException 
	 */
	def writeDataBase(kb: KnowledgeBase, sc: SparkContext) {

	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	  
		try{
		  
		  //Create a directory and a file
		  Mediator.createDBfolder(sc)
		  
		  // Write variables and class labels
  		val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(new Path(Mediator.getLearnerDataBasePath()+"//DataBase000.txt"))){
			  kb.saveDBFile(Mediator.getLearnerDataBasePath()+"//DataBase000.txt", sc)
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(Mediator.getLearnerDataBasePath()+"//DataBase"+aux+i.toString+".txt"))){
    			  kb.saveDBFile(Mediator.getLearnerDataBasePath()+"//DataBase"+aux+i.toString+".txt", sc)
    			  create = true
    		  }   
		      
		      i = i + 1
		    }
		  }
			
		}catch{
		  case e: Exception => {
		    println("\nERROR WRITING RULE BASE (URI)")
			  e.printStackTrace()
			  System.exit(1)
		  }
		}

		/**
		 * TEMPORAL Write info about the number of each rules
		 */  
		try {
		  
		  import org.apache.hadoop.conf.Configuration
      import org.apache.hadoop.fs.{FileSystem, Path}    
      import java.net.URI
      import java.io.{BufferedWriter, OutputStreamWriter}
    
		  var counter_outputFile: String = Mediator.getLearnerOutputPath()
			Mediator.createInformationfolder(sc)
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
  		var textPath: Path = null
		  if(!fs.exists(new Path(counter_outputFile+"//Information//CounterRules000.txt"))){
			  textPath = new Path(counter_outputFile+"//Information//CounterRules000.txt")
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(counter_outputFile+"//Information//CounterRules"+aux+i+".txt"))){
    			  textPath = new Path(counter_outputFile+"//Information//CounterRules"+aux+i+".txt")
    			  create = true
    		  }
		      i = i + 1
		    }
		    
		  }
		  
		  var bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath,true)))

		  var counter = kb.getCounterRules()
		  var positive, negative = 0.0
		  for (index <- 0 to (counter.length - 1)){
		    positive = positive + counter(index).getPositive().toDouble
		    negative = negative + counter(index).getNegative().toDouble
		  }
		  
		  bwText.write(" Average of RB in each Map with total Map = " + counter.length.toString + "\n")
	    bwText.write(" Class "+ kb.getDataBase().getClassLabel(0) + " = " + (positive/counter.length.toDouble).toString + "\n")
	    bwText.write(" Class "+ kb.getDataBase().getClassLabel(1) + " = " + (negative/counter.length.toDouble).toString + "\n")
		  
    	bwText.close()
      
		}catch{
		  case e: Exception => {
	    System.err.println("\nMAPPER: ERROR WRITING TEMPORAL INFO IN RULES GENERATION")
		  e.printStackTrace()
		  System.err.println(-1)
		 }
		}
	}

	/**
	 * Writes the data base in text format in a file (specified by Mediator.DATA_BASE_OUTPUT_PATH)
	 * @param printStdOut if true the data base is printed in the standard output
	 * @throws IOException 
	 */
	def writeDataBaseText (/*printStdOut: Boolean*/) /*throws IOException*/ {

		/*try{
			FileSystem fs = FileSystem.get(new URI(Mediator.getHDFSLocation()), Mediator.getConfiguration())
			Path pt = new Path(Mediator.getHDFSLocation()+Mediator.getLearnerDatabasePath()+"_text.txt")
			//FileSystem fs = FileSystem.get(Mediator.getConfiguration())
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)))
			bw.write("\nDATA BASE ("+Mediator.getNumVariables()+" variables):\n\n")
			if (printStdOut)
				System.out.println("\nDATA BASE ("+Mediator.getNumVariables()+" variables):\n")
			for (Variable variable:Mediator.getVariables()){
				bw.write(variable+"\n")
				if (printStdOut)
					System.out.println(variable)
			}
			bw.close()
			fs.close()
		}catch(Exception e){
			System.err.println("\nERROR WRITING RULE BASE (URI)")
			e.printStackTrace()
		}*/

	}

	/**
	 * Writes total execution time
	 */
	def writeExecutionTime(startMs: Long, endMs: Long, sc: SparkContext) {

	  import java.io.{BufferedReader, File, InputStreamReader}
	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
    import java.io.{BufferedWriter, OutputStreamWriter}
		
		var elapsed: Long = endMs - startMs
		var hours: Long = (elapsed / 3600000)
		var minutes: Long = (elapsed % 3600000) / 60000
		var seconds: Long = ((elapsed % 3600000) % 60000) / 1000

		try {
		  
		  var textPath:Path = null
		  
		  val conf = sc.hadoopConfiguration
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(new Path(Mediator.getLearnerOutputPath()+"//Time000.txt"))){
		    textPath = new Path(Mediator.getLearnerOutputPath()+"//Time000.txt")        	    
		  }else {
		    
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    
		    while(i<100 && !create){
		      if (i == 10)
		        aux = "0"
		      
		      if(!fs.exists(new Path(Mediator.getLearnerOutputPath()+"//Time"+aux+i.toString+".txt"))){
    			  textPath = new Path(Mediator.getLearnerOutputPath()+"//Time"+aux+i.toString+".txt")
    			  create = true
    		  }   
		      
		      i = i + 1
		    }
		  }
		  
		  val bwText = new BufferedWriter(new OutputStreamWriter(fs.create(textPath)))

			/**
			 * Write total execution time
			 */
			bwText.write("Total execution time (hh:mm:ss): "+("00" + hours.toString()).substring(hours.toString().length())+":"
			                                                +("00" + minutes.toString()).substring(minutes.toString().length())+":"
			                                                +("00" + seconds.toString()).substring(seconds.toString().length())
					                                            +" ("+(elapsed/1000).toString()+" seconds)\n")

			def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
        dir.listFiles.filter(_.isFile).toList.filter { file =>
            extensions.exists(file.getName.endsWith(_))
        }
      }		                                            
			/**
			 *  Write Mappers execution time avg.
			 */
			val directory = new File(Mediator.getLearnerOutputPath()+"//"+Mediator.TIME_STATS_DIR)
			var files = List[File]()
			var extension = List("txt")
			if(directory.exists() && directory.isDirectory()){
			  files = getListOfFiles(directory, extension)			  
			}
		  
			var br: BufferedReader = null
			var buffer: String = ""
			var sumMappers: Double = 0.0
			var numMappers: Int = 0
			files.foreach { file => 
				// Read Stage 1
				if (file.getName().contains("mapper")){
					numMappers = numMappers + 1
					br = new BufferedReader(new InputStreamReader(fs.open(new Path(file.getPath()))))
					buffer = br.readLine()
					sumMappers = sumMappers + buffer.substring(buffer.indexOf(":")+1).trim().toDouble
				}
				br.close()
			}
			// Write AVG
			elapsed = (sumMappers / numMappers).toLong
			hours = elapsed / 3600
			minutes = (elapsed % 3600) / 60
			seconds = (elapsed % 3600) % 60
			
			bwText.write("Mappers avg. execution time (hh:mm:ss): "
			                          +("00" + hours.toString()).substring(hours.toString().length())+":"
			                          +("00" + minutes.toString()).substring(minutes.toString().length())+":"
			                          +("00" + seconds.toString()).substring(seconds.toString().length())
			                          +" ("+elapsed.toString()+" seconds)\n")

			bwText.close()
			
			// Remove time stats directory
			fs.delete(new Path(Mediator.getLearnerOutputPath()+"//"+Mediator.TIME_STATS_DIR),true)
			directory.delete()
  
		}catch{
		  case e: Exception => {
		    println("\nERROR WRITING DATA BASE")
			  e.printStackTrace()
			  System.err.println(-1)
		  }
		}

	}

	/**
	 * Writes the final rule base in a human-readable file
	 * @param printStdOut if true the rule base is printed in the standard output
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	def writeFinalRuleBase (kb: KnowledgeBase, sc: SparkContext, printStdOut: Boolean = false) /*throws IllegalArgumentException, IOException*/{

	  import org.apache.hadoop.fs.{FileSystem, Path}
	  import org.apache.hadoop.conf.Configuration
	  
		try{
		   
		  //Create a directory and a file
		  Mediator.createRBfolder(sc)
		  
		  val conf = sc.hadoopConfiguration
  		//conf.set("RB"+"//RuleBase000.txt", Mediator.getLearnerRuleBasePath()+"//RuleBase000.txt")
  		val fs = FileSystem.get(conf)
		  if(!fs.exists(new Path(Mediator.getLearnerRuleBasePath()+"//RuleBase000.txt"))){
			  kb.saveRBFile(Mediator.getLearnerRuleBasePath()+"//RuleBase000.txt", sc, printStdOut)
  		}
		  else {
		    var create = false
		    var i: Int = 1
		    var aux = "00"
		    while(i<100 && !create){
		      if(i == 10)
		        aux = "0"
		        
		      //conf.set("RB"+"//RuleBase"+aux+i+".txt", Mediator.getLearnerRuleBasePath()+"//RuleBase"+aux+i+".txt")
  		    //fs = FileSystem.get(conf)
		      if(!fs.exists(new Path(Mediator.getLearnerRuleBasePath()+"//RuleBase"+aux+i.toString+".txt"))){
    			  kb.saveRBFile(Mediator.getLearnerRuleBasePath()+"//RuleBase"+aux+i.toString+".txt", sc, printStdOut)
    			  create = true
    		  }   
		      
		      i = i + 1
		    }
		  }
		}catch{
		  case e: Exception => {
		    println("\nERROR WRITING DATA BASE")
			  e.printStackTrace()
			  System.err.println(-1)
		  }
		}
	}
       
	/**
	 * Writes the rule base in a temporal file
	 * @param printStdOut if true the rule base is printed in the standard output
	 */
	def writeRuleBaseTmp (ruleBase: Array[FuzzyRule], sc: SparkContext) {
	  
		try{
		  Mediator.saveTmpRBFile(ruleBase, Mediator.getLearnerRuleBasePath()+"//RuleBaseTmp.txt", sc)
		}catch{
		  case e: Exception => {
		    println("\nERROR WRITING DATA BASE")
			  e.printStackTrace()
			  System.err.println(-1)
		  }
		}
	}
}