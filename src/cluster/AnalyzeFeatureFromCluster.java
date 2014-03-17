package cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Stack;

/*
 * Analyze the min, max, mean, variance of each feature,
 * in each cluster
 * 
 */
public class AnalyzeFeatureFromCluster {
	
	private static int _clusterNum = 0;
	private static int _featureNum = 0;
	
	private static double [][] _minArr = null;
	private static double [][] _maxArr = null;
	private static double [][] _meanArr = null;
	private static double [][] _varArr = null;
	private static int [] _clusterSizeArr = null;
	
	private static boolean [][] _descriptiveSel = null;
	private static boolean [][] _discriminatingSel = null;
	
	public static void selectFeature(String clutoStdoutFile) throws Exception{
		//init arr		
		_descriptiveSel = new boolean[_clusterNum][];
		_discriminatingSel = new boolean[_clusterNum][];
		
		for (int i=0; i<_clusterNum; i++) {	
			_descriptiveSel[i] = new boolean[_featureNum];
			_discriminatingSel[i] = new boolean[_featureNum];
			for (int j=0; j<_featureNum; j++) {
				_descriptiveSel[i][j] = false;
				_discriminatingSel[i][j] = false;	
			}		
		}
		
		BufferedReader br = new BufferedReader(new FileReader(clutoStdoutFile));		
		try {
			//find first feature data line:
			String line;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("Cluster")) {
					break;
				}
			}
			
			int clusterIdx = 0;
			String [] lst;
			while (line != null) {
				//parse cluster id
				int pos = 7; 	//skip "Cluster"
				while (line.charAt(pos) == ' ' || line.charAt(pos) == '\t')
					pos ++;				
				int pos2 = pos;
				while (line.charAt(pos2) != ',')
					pos2 ++;				
				int id = Integer.parseInt(line.substring(pos, pos2));
				if (id != clusterIdx) {
					throw new Exception("wrong cluster id!");
				}
				
				//descriptive line
				line = br.readLine();
				lst = line.split("col");
				for (int i=1; i<lst.length; i++) {
					pos = 0;
					while (lst[i].charAt(pos) != ' ')
						pos ++;
					int colID = Integer.parseInt(lst[i].substring(0, pos));
					_descriptiveSel[clusterIdx][colID-1] = true;
				}
				
				//discriminating line
				line = br.readLine();
				lst = line.split("col");
				for (int i=1; i<lst.length; i++) {
					pos = 0;
					while (lst[i].charAt(pos) != ' ')
						pos ++;
					int colID = Integer.parseInt(lst[i].substring(0, pos));
					_discriminatingSel[clusterIdx][colID-1] = true;
				}
				
				clusterIdx ++;
				line = br.readLine();	//skip empty line
				line = br.readLine();	//read next cluster result
				if (line == null || !line.startsWith("Cluster")) {
					break;
				}
			}			
		}
		finally {
			br.close();
		}
	}
	
	
	public static void analyzeFile(String clutoFeatureFile, 
			String clusterResultFile) throws Exception {
		
		// init arr
		_clusterSizeArr = new int[_clusterNum];
		_minArr = new double[_clusterNum][];
		_maxArr = new double[_clusterNum][];
		_meanArr = new double[_clusterNum][];
		_varArr = new double[_clusterNum][];
		
		for (int i=0; i<_clusterNum; i++) {	
			_clusterSizeArr[i] = 0;
			_minArr[i] = new double[_featureNum];
			_maxArr[i] = new double[_featureNum];
			_meanArr[i] = new double[_featureNum];
			_varArr[i] = new double[_featureNum];
			for (int j=0; j<_featureNum; j++) {
				_minArr[i][j] = Double.MAX_VALUE;
				_maxArr[i][j] = Double.MIN_VALUE;
				_meanArr[i][j] = 0.0;
				_varArr[i][j] = 0.0;		
			}		
		}		
		
		// First pass
		BufferedReader br1 = new BufferedReader(new FileReader(clutoFeatureFile));
		BufferedReader br2 = new BufferedReader(new FileReader(clusterResultFile));		
		try {
			String line1;
			String line2;
			br1.readLine();	//ignore first line of cluto features file			
			while ((line1 = br1.readLine()) != null) {
				if (line1.isEmpty())
					continue;
				line2 = br2.readLine();
				int idx = Integer.parseInt(line2);
				String [] lst = line1.split(" ");
				
				_clusterSizeArr[idx] ++;
				for (int j=0; j<_featureNum; j++) {
					double val = Double.parseDouble(lst[j]);
					
					if (_minArr[idx][j] > val)
						_minArr[idx][j] = val;
					if (_maxArr[idx][j] < val)
						_maxArr[idx][j] = val;
					_meanArr[idx][j] += val;					
				}
			}						
			//write result
		}
		finally {
			br1.close();
			br2.close();			
		}
		
		// Calculate mean
		for (int i=0; i<_clusterNum; i++) {
			for (int j=0; j<_featureNum; j++) {
				_meanArr[i][j] /= _clusterSizeArr[i];
			}
		}
		
		// Second pass
		br1 = new BufferedReader(new FileReader(clutoFeatureFile));
		br2 = new BufferedReader(new FileReader(clusterResultFile));		
		try {
			String line1;
			String line2;			
			br1.readLine();	//ignore first line of cluto features file
			while ((line1 = br1.readLine()) != null) {
				if (line1.isEmpty())
					continue;
				line2 = br2.readLine();
				int idx = Integer.parseInt(line2);
				String [] lst = line1.split(" ");
				
				for (int j=0; j<_featureNum; j++) {
					double val = Double.parseDouble(lst[j]);					
					double diff = val - _meanArr[idx][j];					
					_varArr[idx][j] += diff * diff;					
				}
			}						
			//write result
		}
		finally {
			br1.close();
			br2.close();			
		}
		
		// Calculate variance
		// Calculate mean
		for (int i=0; i<_clusterNum; i++) {
			for (int j=0; j<_featureNum; j++) {
				_varArr[i][j] /= _clusterSizeArr[i];
			}
		}
	}
	
	public static void WriteAnalysisToFile(String dstFile) throws Exception {
		BufferedWriter bw = new BufferedWriter(new FileWriter(dstFile));
		try {
			for (int i=0; i<_clusterNum; i++) {
				bw.write("Cluster id " + i + ":\n");
				
				bw.write("\t");
				for (int j=0; j<_featureNum; j++) {
					if (!_descriptiveSel[i][j]) continue;
					int idx = j+1;
					bw.write("f" + idx + "\t");
				}
				bw.write("\n");
				
				bw.write("Min:\t");
				for (int j=0; j<_featureNum; j++) {
					if (!_descriptiveSel[i][j]) continue;
					bw.write(_minArr[i][j] + "\t");
				}
				bw.write("\n");
				
				bw.write("Max\t");
				for (int j=0; j<_featureNum; j++) {
					if (!_descriptiveSel[i][j]) continue;
					bw.write(_maxArr[i][j] + "\t");
				}
				bw.write("\n");
				
				bw.write("Mean:\t");
				for (int j=0; j<_featureNum; j++) {
					if (!_descriptiveSel[i][j]) continue;
					bw.write(_meanArr[i][j] + "\t");
				}
				bw.write("\n");
				
				bw.write("Var:\t");
				for (int j=0; j<_featureNum; j++) {
					if (!_descriptiveSel[i][j]) continue;
					bw.write(_varArr[i][j] + "\t");
				}
				bw.write("\n");
				
				bw.write("\n");
			}
		}
		finally {
			bw.close();
		}
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
	    if (args.length < 6) {
	      System.err.println("Usage: " + className + " <cluster-num> <feature-num> " +
	      		"<cluto-stdout-file> <cluto-feature-file> <cluster-result-file> <dstfile>");
	      System.exit(2);
	    } 
	    
	    _clusterNum = Integer.parseInt(args[0]);
	    _featureNum = Integer.parseInt(args[1]);	    	  
	    
	    System.out.println("Selecting descriptive and discriminating features for each cluster...");
	    selectFeature(args[2]);
	    
	    System.out.println("Analyzing features for each cluster...");
	    analyzeFile(args[3], args[4]);
	    
	    System.out.println("Writing analysis to file...");
	    WriteAnalysisToFile(args[5]);
	    
	    System.out.println("Finished");
	}	
}
