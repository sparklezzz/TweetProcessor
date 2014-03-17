package cluster;

class Node<T extends Comparable<T>, V> {
	 public T key;
	 public V val;
	 
	 public Node(T key){
	  this.key = key;
	  this.val = null;
	 }
	 public Node(T key, V val){
		 this.key = key;
		 this.val = val;
	 }
	 
	 public T getKey() {
	  return key;
	 }
	 public void setKey(T key) {
	  this.key = key;
	 }
}