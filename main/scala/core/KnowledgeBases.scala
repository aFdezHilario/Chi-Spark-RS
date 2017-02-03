package core

class KnowledgeBases {
  
	 private var knowledgeBase: Array[KnowledgeBase] = null
	 
	 def this(kb: KnowledgeBase){
	   
	   this()
	   if(this.knowledgeBase == null)
	     knowledgeBase = Array[KnowledgeBase]()
		 this.knowledgeBase = Array[KnowledgeBase]()
		 this.knowledgeBase = this.knowledgeBase :+ kb.clone()
		 
	 }
	 
	 def addKB(kb: KnowledgeBase){
		 this.knowledgeBase = this.knowledgeBase :+ kb.clone()
	 }
	 
	 def size(): Int = {
		 this.knowledgeBase.length
	 }
	 
	 def get(id: Int): KnowledgeBase = {
		 this.knowledgeBase(id)
	 }
}
