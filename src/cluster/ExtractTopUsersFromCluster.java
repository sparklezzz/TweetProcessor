package cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

public class ExtractTopUsersFromCluster {

	//private static HashMap<Long, Long> _PRMap = new HashMap<Long, Long>();
	//private static HashMap<Long, String> _userMap = new HashMap<Long, String>();
	//private static long _maxID = 0;
	private static MinHeap<Double, String> []  _minHeapArr = null;
	private static int _clusterNum = 0;
	private static int _minHeapSize = 0;
	/*
	public static boolean loadIDMapAndUserMap(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_PRMap.clear();
		_userMap.clear();
		String line = null;
		long curID = 1;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			int pos = line.indexOf("\t");	
			long globalID = Long.parseLong(line.substring(pos + 1));
			_PRMap.put(curID, globalID);
			_userMap.put(curID, line.substring(0, pos));
			curID ++;
		}				
		br.close();	
		_maxID = curID;
		return true;
	}
	*/
	public static void InitHeapArr(int clusterNum, int maxSize) {
		_clusterNum = clusterNum;
		_minHeapSize = maxSize;
		_minHeapArr = new MinHeap[clusterNum];
		for (int i=0; i<clusterNum; i++) {
			_minHeapArr[i] = new MinHeap<Double, String>(maxSize);
		}
	}
	
	public static void updateHeap(MinHeap<Double, String> minHeap, Double key, String val) {		
		if (!minHeap.isFull()) {
			minHeap.insert(key, val);
		}
		else {
			if (minHeap.top().key < key) {	//change the mininum element to a larger one
				minHeap.remove();
				minHeap.insert(key, val);
			}
		}
	}
	
	public static void getTopUser(String userDictFile, 
			String clusterResultFile) throws Exception {
		BufferedReader br1 = new BufferedReader(new FileReader(userDictFile));
		BufferedReader br2 = new BufferedReader(new FileReader(clusterResultFile));
		
		try {
			String line1;
			String line2;
			while ((line1 = br1.readLine()) != null) {
				if (line1.isEmpty())
					continue;
				line2 = br2.readLine();
				int idx = Integer.parseInt(line2);
				String [] lst = line1.split("\t");
				updateHeap(_minHeapArr[idx], new Double(lst[1]), lst[0]);
			}						
			//write result
		}
		finally {
			br1.close();
			br2.close();
			
		}
	}
	
	public static void WriteTopUsersToFile(String dstFile) throws Exception {
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(dstFile + "_raw"));
		Stack<Node<Double, String> > stack = new Stack<Node<Double, String> >();
		try {
			for (int i=0; i<_clusterNum; i++) {
				bw.write("Cluster id " + i + ":\n");
				stack.clear();
				while (!_minHeapArr[i].isEmpty()) {					
					Node<Double, String> n = _minHeapArr[i].remove();
					stack.push(n);					
				}
				while (!stack.isEmpty()) {
					Node<Double, String> n = stack.pop();
					bw.write(n.val + "\t" + n.key + "\n");
					bw2.write(n.val + "\t" + n.key + "\n");
				}				
				bw.write("\n");
			}
		}
		finally {
			bw.close();
			bw2.close();
		}
	}
	
	public static void HeapTest() {
		 MinHeap<Integer, String> heap = new MinHeap<Integer, String>(10);
		  heap.insert(3);
		  heap.insert(9);
		  heap.insert(8);
		  heap.insert(12);
		  heap.insert(6);
		  heap.insert(15);
		  heap.display();
		  heap.remove();
		  heap.display();
		  heap.heapSort();
	}	
	
	public static void main(String [] args) throws Exception{
		//HeapTest();
		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
	    if (args.length < 5) {
	      System.err.println("Usage: " + className + " <cluster-num> <top-user-num> " +
	      		"<local-user-pr-file> <cluster-result-file> <dstfile>");
	      System.exit(2);
	    } 
	    
	    System.out.println("Initing heap arr...");
	    InitHeapArr(Integer.parseInt(args[0]), Integer.parseInt(args[1]));	    	  
	    
	    System.out.println("Getting top users for each cluster...");
	    getTopUser(args[2], args[3]);
	    
	    System.out.println("Writing top users to file...");
	    WriteTopUsersToFile(args[4]);
	    
	    System.out.println("Finished");
	}	
	
}
