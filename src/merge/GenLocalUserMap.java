package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

public class GenLocalUserMap {

	public static HashMap<String, String> _globalUser2IDMap = new HashMap<String, String>();
	
	public static boolean loadGlobalUserDictMap(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_globalUser2IDMap.clear();
		String line = null;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			int pos = line.indexOf("\t");			
			if (pos != -1) {
				_globalUser2IDMap.put(line.substring(0, pos), line.substring(pos+1));
			}
		}				
		br.close();	

		return true;
	}
		
	public static void processFile(String srcFile, String dstFile) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		try {
			String line;
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				
				int pos = line.indexOf("\t");
				String userStr;
				if (pos == -1) {
					userStr = line;
				}
				else {
					userStr = line.substring(0, pos);
				}
				// We assume that userStr must appear in global dict!
				String globalID = _globalUser2IDMap.get(userStr);
				bw.write(userStr + "\t" + globalID + "\n");
			}
		}
		finally {
			br.close();
			bw.close();
		}
	}
		
	public static void main(String [] args) throws Exception {		
		String className = new Object()    {
		 	 public String getClassName() 
			 {
			     String clazzName = this.getClass().getName();
			     return clazzName.substring(0, clazzName.lastIndexOf('$'));
			 }
			 }.getClassName();
	    if (args.length < 3) {
	      System.err.println("Usage: " + className + " <global_user_id_map_on_local_fs> <local_user_dict> <dst_file>");
	      System.exit(2);
	    } 
	    
	    System.out.println("Loading global dict...");
	    loadGlobalUserDictMap(args[0]);
	    
	    System.out.println("Process local dict...");
		processFile(args[1], args[2]);
	    
		System.out.println("Completed.");
	}
	
}
