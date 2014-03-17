package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

public class SingleFeatureCompleter {

	public static HashMap<String, Long> _user2IDMap = new HashMap<String, Long>();
	public static long _maxID = 0;	
	
	public static boolean loadUserDictMap(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_user2IDMap.clear();
		String line = null;
		long curID = 1;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			int pos = line.indexOf("\t");			
			if (pos != -1) {
				_user2IDMap.put(line.substring(0, pos), curID);
			}
			else {
				_user2IDMap.put(line, curID);
			}
			curID ++;
		}				
		br.close();	
		_maxID = curID;
		return true;
	}
	
	public static void processSingleFile(String srcFile) throws Exception{
		String dstFile = srcFile + "_useID";
		
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		String line = null;
		long lastID = 1;
		
		try {		
			line = br.readLine();
			if (line == null) return;

			int pos = line.indexOf("\t");
			if (pos == -1) throw new Exception("null value!");
			String userStr = line.substring(0, pos);
			String rest = line.substring(pos + 1);
			
			String []lst = rest.split("\t");
			int len = lst.length;
			if (lst[len-1].isEmpty()) {	//remove last null element 
				len --;
			}

			String blankStr = "0";
			for (int i=1; i<len; i++) {
				blankStr += " 0";
			}
			
			long id = _user2IDMap.get(userStr);
			while (lastID < id) {
				bw.write(blankStr + "\n");
				lastID ++;
			}
			lastID = id + 1;
			bw.write(rest.replace('\t', ' ').trim() + "\n");
						
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				
				pos = line.indexOf("\t");
				if (pos == -1)
					throw new Exception("null value!");
				userStr = line.substring(0, pos);
				rest = line.substring(pos + 1);			
				
				id = _user2IDMap.get(userStr);
				if (lastID > id) 
					throw new Exception("Error! either user dict or feature file is not sorted!");
				
				while (lastID < id) {
					bw.write(blankStr + "\n");
					lastID ++;
				}
				lastID = id + 1;
				bw.write(rest.replace('\t', ' ').trim() + "\n");
			}
			
			// sweep
			while (lastID < _maxID) {
				bw.write(blankStr + "\n");
				lastID ++;
			}
		}
		finally {
			br.close();
			bw.close();
		}
	}
	
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
	    if (args.length < 2) {
	      System.err.println("Usage: " + className + " <sorted_user_dict_path_on_local_fs> <srcfile*>");
	      System.exit(2);
	    } 
	    
	    System.out.println("Loading dict...");
	    loadUserDictMap(args[0]);
	    
	    for (int i=1; i<args.length; i++) {
	    	System.out.println("Processing file " + args[i] + "...");
	    	processSingleFile(args[i]);
	    }
	    
	    System.out.print("Finished.");
	}
	
}
