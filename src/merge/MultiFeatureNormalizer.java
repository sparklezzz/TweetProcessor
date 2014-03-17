package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.HashMap;

/*
 * 1. NaN -> 0
 * 2. Normalizing specified column to [0, 1]
 */
public class MultiFeatureNormalizer {

	public static HashMap<Long, Long> _IDMap = new HashMap<Long, Long>();
	public static HashMap<Long, String> _userMap = new HashMap<Long, String>();
	
	public static long _maxID = 0;
	public static int _featureNum = 0;
	public static final String _SEP = " ";
	
	public static boolean [] _needNormArr = null;
	public static double [] _maxArr = null;
	
	public static boolean loadIDMapAndUserMap(String userDictFile) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(userDictFile));
		_IDMap.clear();
		_userMap.clear();
		String line = null;
		long curID = 1;
		while ((line = br.readLine()) != null) {
			if (line.isEmpty())
				continue;
			int pos = line.indexOf("\t");	
			long globalID = Long.parseLong(line.substring(pos + 1));
			_IDMap.put(curID, globalID);
			_userMap.put(curID, line.substring(0, pos));
			curID ++;
		}				
		br.close();	
		_maxID = curID;
		return true;
	}
	
	public static void initNormArr(String srcFile) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		_needNormArr = new boolean[_featureNum];
		_maxArr = new double[_featureNum];
		for (int i=0; i<_featureNum; i++) {
			_needNormArr[i] = false;
			_maxArr[i] = 0.0;
		}
		int idx = 0;
		try {
			String line;
			while ((line = br.readLine()) != null) {
				if (line.charAt(0) == '1')
					_needNormArr[idx] = true;
				idx ++;
				if (idx == _featureNum)
					break;
			}
		}
		finally {
			br.close();
		}
	}
	
	public static void processMultiFiles(String srcFile, String dstFile) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		//BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		// first pass 
		try {
			String line;
			String [] lst;		
			long curID = 1;	
			while ((line = br.readLine()) != null) {
				if (line.isEmpty()) {
					throw new Exception("Empty line detected in line " + curID);
				}
				
				lst = line.split(_SEP);
				if (lst.length != _featureNum) {
					System.out.println("wrong feature num detected in line "
									+ curID + ": " + lst.length);
					//throw new Exception("wrong feature num detected in line "
					//		+ curID + ": " + lst.length);
				}
				
				for (int i=0 ;i<_featureNum; i++) {
					if (lst[i].equals("NaN"))
						continue;
					double val = Double.parseDouble(lst[i]);
					if (_needNormArr[i] && _maxArr[i] < val)
						_maxArr[i] = val;
				}
				curID ++;
			}
		}
		finally {
			br.close();
			//bw.close();
		}
		
		// second pass
		BufferedReader br2 = new BufferedReader(new FileReader(srcFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		// first pass 
		try {
			String line;
			String [] lst;
			long curID = 1;			
			
			while ((line = br2.readLine()) != null) {
				long globalID = _IDMap.get(curID);
				String userStr = _userMap.get(curID);
				bw.write(curID + _SEP + userStr + _SEP + globalID + _SEP);
				lst = line.split(_SEP);
				for (int i=0 ;i<_featureNum; i++) {
					if (lst[i].equals("NaN")) {
						bw.write("0");
					}
					else if (_needNormArr[i]) {
						double val = Double.parseDouble(lst[i]) / _maxArr[i];
						bw.write(new Double(val).toString());
					}
					else {
						bw.write(lst[i]);
					}

					if (i != _featureNum - 1) {
						bw.write(_SEP);
					}
				}
				bw.write("\n");
				curID ++;
			}
		}
		finally {
			br2.close();
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
	    if (args.length < 5) {
	      System.err.println("Usage: " + className + " <sorted_user_dict_path_on_local_fs> <feature-num> <normalize-list-file> <srcfile> <dstfile>");
	      System.exit(2);
	    } 
	    
	    System.out.println("Loading dict...");
	    loadIDMapAndUserMap(args[0]);
	    
	    _featureNum = Integer.parseInt(args[1]);
	    
	    System.out.println("Init need normalization arr...");
	    initNormArr(args[2]);
	    
	    System.out.println("Normalizing files...");
    	processMultiFiles(args[3], args[4]);
	    
	    System.out.print("Finished");
	}
}
