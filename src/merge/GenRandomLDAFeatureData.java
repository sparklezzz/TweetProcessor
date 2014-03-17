package merge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Random;

public class GenRandomLDAFeatureData {

	public static int _featureNum = 0;
	
	public static void processFile(String srcFile, String dstFile) throws Exception {
		
		BufferedReader br = new BufferedReader(new FileReader(srcFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		
		String line = null;
		
		try {											
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				bw.write("1 ");
				for (int i=0; i<_featureNum; i++) {
					int idx = i+1;
					bw.write("x" + idx + ":");
					double val = Math.random();
					bw.write(val + " ");
				}				
				bw.write("# " + line + "\n");				
			}			
		}
		finally {
			br.close();
			bw.close();
		}
	}
	
	
	public static void main (String [] args) throws Exception{
	
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
	    if (args.length < 3) {
	      System.err.println("Usage: " + className + " <srcfile> <feature-num> <dstfile>");
	      System.exit(2);
	    } 
	    
	    _featureNum = Integer.parseInt(args[1]);
	    processFile(args[0], args[2]);
	    
	}
}
