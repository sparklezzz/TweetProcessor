package cluster;

class MinHeap<T extends Comparable<T>, V>{
	 private int maxSize;
	 private int currentSize;
	 private Node<T, V>[] heapArray;
	 
	 public MinHeap(int max){
	  this.maxSize = max;
	  heapArray = new Node[maxSize];
	  currentSize = 0;
	 }
	 
	 public boolean isEmpty(){
	  return currentSize == 0;
	 }
	 
	 public boolean isFull() {
		 return currentSize == maxSize;
	 }

	 public Node<T,V> top() {
		 return heapArray[0];
	 }
	 
	//插入数据
	 public boolean insert(T key){
	  if(currentSize == maxSize){
	   System.out.println("堆已满，没法插入数据！");
	   return false;
	  }
	  Node<T, V> newNode = new Node<T, V>(key);
	  heapArray[currentSize] = newNode;
	  trickUp(currentSize++);
	  return true;	  
	 }	 
	 
		//插入数据
	 public boolean insert(T key, V val){
	  if(currentSize == maxSize){
	   System.out.println("堆已满，没法插入数据！");
	   return false;
	  }
	  Node<T, V> newNode = new Node<T, V>(key, val);
	  heapArray[currentSize] = newNode;
	  trickUp(currentSize++);
	  return true;	  
	 }

	//在插入数据时，会产生不是堆的情况，需要自底向上比较。
	 public void trickUp(int index){
	  Node<T, V> bottom= heapArray[index];
	  int parent = (index-1)/2;
	  
	  while(index>0 && bottom.getKey().compareTo(heapArray[(parent-1)/2].getKey()) < 0){
	   heapArray[index] = heapArray[parent];
	   index = parent;
	   parent = (parent-1)/2;
	  }
	  heapArray[index] = bottom;
	 }
	 public Node<T, V> remove(){
	  Node<T, V> root = heapArray[0];
	  heapArray[0] = heapArray[--currentSize];
	  trickDown(0);
	  return root;
	  
	 }

	//删除数据时，也是一样，但是是自上向下比较
	 private void trickDown(int index) {
	  Node<T, V> top = heapArray[index];
	  int largeChild;
	  while(index < currentSize/2){
	   int leftChild = 2*index+1;
	   int rightChild = 2*index+2;
	   if(rightChild < currentSize && (heapArray[leftChild].getKey().compareTo(heapArray[rightChild].getKey())>0)){
	    largeChild = rightChild;
	   }else{
	    largeChild = leftChild;
	   }
	   if(top.getKey().compareTo(heapArray[largeChild].getKey()) <= 0){
	    break;
	   }
	   heapArray[index] = heapArray[largeChild];
	   index = largeChild;
	  }
	  heapArray[index] = top;
	 }
	 public void display(){
	  for(int i=0;i<currentSize;i++){
	   System.out.print(heapArray[i].getKey()+"  ");
	  }
	  System.out.println();
	 }
	 //利用每次删除的都是最大的那个节点，可以对堆排序
	 public void heapSort(){
	  while(currentSize>0){
	   System.out.print(this.remove().getKey()+" ");
	  }
	  System.out.println();
	 }
	 
}
	


