package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class ClutoFeatureTransformer {
		
	public static long _maxID = 0;
	public static int _userNum = 0;
	public static int _featureNum = 0;
	public static final String _SEP = " ";		
	public static HashMap<String, String> _ClutoUserMap = new HashMap<String, String>();
	public static ArrayList<String> _ClutoUserList = new ArrayList<String>();
	
	public static boolean loadClutoUser(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_ClutoUserMap.clear();
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			
			String [] lst = line.split(" |\t");
			
			_ClutoUserMap.put(lst[0], "");
			_ClutoUserList.add(lst[0]);
		}				
		br.close();	
		return true;
	}
	
	public static void fillClutoUserData(String srcFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		try {
			String line;
			String [] lst;
			while ((line = br.readLine()) != null) {			
				lst = line.split(_SEP);
				if (_ClutoUserMap.containsKey(lst[1])) {
					_ClutoUserMap.put(lst[1], line);
				}
			}
		}
		finally {
			br.close();
		}
	}	
	
	public static void WriteClutoUserDataToFile(String dstFile) throws Exception{		
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));		
		// first pass
		
		int count = 0;
		try {			
			bw.write(_userNum + _SEP + _featureNum + "\n");
			
			String [] lst;		
			
			for (String userStr : _ClutoUserList) {							
				String rawData = _ClutoUserMap.get(userStr);
				
				lst = rawData.split(_SEP);
				for (int i=3 ;i<_featureNum + 3; i++) {	// skip local id, userStr, global id					
					bw.write(lst[i]);

					if (i != _featureNum + 2) {
						bw.write(_SEP);
					}
				}
				bw.write("\n");
				count ++;
			}
		}
		catch (Exception e) {
			throw new Exception("wrong line count: " + count);
		}
		finally {
			bw.close();
		}
		
	}
	/*
	public static void processMultiFiles(String srcFile, String dstFile) throws Exception{
		
		// second pass
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		// first pass 
		try {
			bw.write(_userNum + _SEP + _featureNum + "\n");
			
			String line;
			String [] lst;
			long curID = 1;			
			
			while ((line = br.readLine()) != null) {				
				lst = line.split(_SEP);
				for (int i=3 ;i<_featureNum + 3; i++) {	// skip local id, userStr, global id					
					bw.write(lst[i]);

					if (i != _featureNum + 2) {
						bw.write(_SEP);
					}
				}
				bw.write("\n");
				curID ++;
			}
		}
		finally {
			br.close();
			bw.close();
		}
		
	}
	*/
	public static void main(String [] args) throws Exception{
	    /**
	     * 这里必须有输入/输出
	     */
	    
	    String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
	    if (args.length < 5) {
	      System.err.println("Usage: " + className + " <line-num> <feature-num> <cluto-user-file> <srcfile> <dstfile>");
	      System.exit(2);
	    } 	   
	    _userNum = Integer.parseInt(args[0]);
	    _featureNum = Integer.parseInt(args[1]);
	    	    
	    System.out.println("Loading Cluto users list...");
	    loadClutoUser(args[2]);
	    
	    System.out.println("fill Cluto User feature data...");
	    fillClutoUserData(args[3]);
	    
	    System.out.println("Writing Cluto User data to file...");
	    WriteClutoUserDataToFile(args[4]);
	    
	    System.out.print("Finished");
	}	
}
