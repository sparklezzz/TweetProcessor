package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class LDAFeatureTransformer {

	public static HashMap<String, String> _LDAUserMap = new HashMap<String, String>();
	public static ArrayList<String> _LDAUserList = new ArrayList<String>();
	public static int _featureNum = 0;
	public static final String _SEP = " ";
	
	public static boolean loadLDAUser(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_LDAUserMap.clear();
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			
			String [] lst = line.split("\t| ");
			_LDAUserMap.put(lst[0], "");
			_LDAUserList.add(lst[0]);					
		}				
		br.close();	
		return true;
	}
	
	public static void fillLDAUserData(String srcFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		try {
			String line;
			String [] lst;
			while ((line = br.readLine()) != null) {			
				lst = line.split(_SEP);
				if (_LDAUserMap.containsKey(lst[1])) {
					//System.out.println(lst[1]);
					_LDAUserMap.put(lst[1], line);
				}
			}
		}
		finally {
			br.close();
		}
	}	
	
	public static void WriteLDAUserDataToFile(String dstFile) throws Exception{		
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));		
		// first pass 
		int count = 0;
		try {			
			String [] lst;		
			
			for (String userStr : _LDAUserList) {							
				String rawData = _LDAUserMap.get(userStr);
				//System.out.println(rawData);
				lst = rawData.split(" ");
				bw.write("1 ");
				for (int i=0; i<_featureNum; i++) {
					int idx = i+1;
					bw.write("f" + idx + ":" + lst[i+3] + " ");
				}
				bw.write("# " + userStr + "\n");	
				count ++;
			}
		}
		catch (Exception e) {
			System.out.println("Error line: " + (count + 1));
			e.printStackTrace();
			throw new Exception();
		}
		finally {
			bw.close();
		}
		
	}
	
	public static void main(String [] args) throws Exception{
	    String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
	    if (args.length < 4) {
	      System.err.println("Usage: " + className + " <feature-num> <LDA-user-file> <normalized-mutli-feature-file> <dstfile>");
	      System.exit(2);
	    } 	   
	    _featureNum = Integer.parseInt(args[0]);
	    	  
	    System.out.println("Loading LDA users list...");
	    loadLDAUser(args[1]);
	    
	    System.out.println("fill LDA User feature data...");
	    fillLDAUserData(args[2]);
	    
	    System.out.println("Writing LDA User data to file...");
	    WriteLDAUserDataToFile(args[3]);
	    
	    System.out.println("Finished");
	}	
}
