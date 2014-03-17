package feature;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import util.PluralRemovalStemmer;

public class InternalInLinkSubnetExtractor {
    
	public static class StrLongPair implements WritableComparable<StrLongPair> {
		String first;
		long second;

		public StrLongPair() {

		}

		public void set(String first, long second) {
			this.first = first;
			this.second = second;
		}

		public String toString() {
			return first + "|" + second;
		}

		public String getFirst() {
			return first;
		}

		public long getSecond() {
			return second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			int length = first.length();	
			byte[] buf = first.getBytes();	
			// 先写字符串长度	
			out.writeInt(length);	
			// 再写字符串数据	
			out.write(buf, 0, length);	
			// 接着long
			out.writeLong(second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// 先读字符串的长度信息	
			int length = in.readInt();	
			byte[] buf = new byte[length];	
			in.readFully(buf, 0, length);	
			first = new String(buf);	
			second = in.readLong();
		}

		@Override
		public int hashCode() {
			return first.hashCode();
		}

		@Override
		public boolean equals(Object right) {
			if (right == null)  
                return false;  
            if (this == right)  
                return true;  
            if (right instanceof StrLongPair) {  
                StrLongPair r = (StrLongPair) right;  
                return r.first.equals(first) && r.second == second;  
            } else {  
                return false;  
            }  
		}

		@Override
		public int compareTo(StrLongPair o) {
			int c1 = first.compareTo(o.first);
			if (c1 != 0) {	
				return c1;	
			} else {
				if (second < o.second)
					return -1;
				else if (second > o.second)
					return 1;
				else
					return 0;	
			}
		}		
	}	
	
	/** 
     * 分区函数类。根据first确定Partition。 
     */  
    public static class FirstPartitioner extends Partitioner<StrLongPair,Text>{  
      @Override  
      public int getPartition(StrLongPair key, Text value,   
                              int numPartitions) {  
        return Math.abs(key.first.hashCode() * 127) % numPartitions;  
      }  
    }  	
	
    public static class GroupingComparator extends WritableComparator {  
        protected GroupingComparator() {  
          super(StrLongPair.class, true);  
        }  
        @Override  
        //Compare two WritableComparables.  
        public int compare(WritableComparable w1, WritableComparable w2) {  
          StrLongPair ip1 = (StrLongPair) w1;  
          StrLongPair ip2 = (StrLongPair) w2;  
          String l = ip1.getFirst();  
          String r = ip2.getFirst();  
          int res = l.compareTo(r);
          return res;      	  
        }  
      }      	
	
	
  public static class TweetMapper
       extends Mapper<Object, Text, StrLongPair, Text>{
	  private final Text newVal = new Text(); 
      private PluralRemovalStemmer stemmer = new PluralRemovalStemmer();
	  private StringBuffer _buffer = new StringBuffer();  
      
	  
	  /*
	   * (k1, v1) : <lineoffset, (d, ns, s1, s2, ..., sns)>
	   * 
	   * (k2, v2) : <d, (d, ns, s1, s2, ..., sns)>
	   * 			<s1, (d, ns, s1, s2, ..., sns)>
	   * 			<s2, (d, ns, s1, s2, ..., sns)>
	   * 			...
	   * 			<sns, (d, ns, s1, s2, ..., sns)>
	   */
      public void map(Object key, Text value, Context context)  
              throws IOException, InterruptedException {
    	  String line = value.toString();  
          int pos = line.indexOf("\t");

          StrLongPair strLongPair = new StrLongPair();
          if (pos != -1) {          	 
        	  String to = line.substring(0, pos);
        	  String fromListStr = line.substring(pos + 1);
        	  
        	  String []fromList = fromListStr.split("\t");
        	  int fromListNum = Integer.parseInt(fromList[0]);
        	  
        	  newVal.set(line);
        	  /*
        	   * The reason we use StrLongPair type as key is as follows:
        	   * 0 -> the TO user himself
        	   * 1 -> the other FROM user
        	   * With the help of FirstComparator and GroupComparator,
        	   * we guarantee that the record with "0" in its StrLongPair key must
        	   * appear first in the list of values!
        	   */
        	  strLongPair.set(to, 0);		      	  
        	  context.write(strLongPair, newVal);
        	  String from;
        	  for (int i = 1; i <= fromListNum; i++) {
        		  from = fromList[i];
        		  if (!from.isEmpty()) {
        			  strLongPair.set(from, 1);
        			  context.write(strLongPair, newVal);
        		  }
        	  }        	          	          	                
          }  
      }  
  }
  
  public static class TweetReducer
    extends Reducer<StrLongPair, Text, Text, Text> {
	private final Text newKey = new Text();
	private final Text newVal = new Text();
	private HashSet<String> _selfFromSet = new HashSet<String>(); 
	
	
	/*
	 * 
	 * (k3, v3): <center, (src, dst)>
	 * Note that center has two inlinks, from src and dst respectively.
	 * Also note that either or dst can be eqaul to center!
	 * 	 
	 */
    public void reduce(StrLongPair key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {            
          
    	_selfFromSet.clear();
    	
    	//get the first record ---- Link of the user himself
    	String selfRec = values.iterator().next().toString();
    	String []selfFromList = selfRec.split("\t");
    	if (!key.first.equals(selfFromList[0])) {
    		return;	// this user have no inlink!  
    	}
    	
    	//ignore the 1th element: degree
    	for (int i=2; i<selfFromList.length; i++) {
    		_selfFromSet.add(selfFromList[i]);
    	}
    	
    	for (Text val : values) {
    		String otherRec = val.toString();
    		String []otherFromList = otherRec.split("\t");
        	if (key.first.equals(otherFromList[0])) {
        		continue;	// ignore self-loop-link
        		//throw new IOException("Rest record in the group owns to the user himself!\n" +
        		//		" Key.first: " + key.first +
        		//		" Key.second: " + new Long(key.second).toString() +
        		//		" otherToList[0]: " + otherToList[0]);
        	}
        	        	
        	newKey.set(otherFromList[0]);
        	newVal.set(key.first + "\t" + otherFromList[0]);	// This link must exist!
        	context.write(newKey, newVal);
        	
        	if (_selfFromSet.contains(otherFromList[0])) {
        		newVal.set(otherFromList[0] + "\t" + key.first);
    			context.write(newKey, newVal);
        	}
        	
        	for (int i=2; i<otherFromList.length; i++) {
        		String candidate = otherFromList[i];
        		if (!key.first.equals(candidate) && _selfFromSet.contains(candidate)) {
        			newVal.set(candidate + "\t" + key.first);
        			context.write(newKey, newVal);
        		}
        	}        	
    	}    	    	      
    }
  }
  
  public static class TweetMapper2
  extends Mapper<Object, Text, Text, Text>{
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();  
  
	  public void map(Object key, Text value, Context context)  
              throws IOException, InterruptedException {  
    	  String line = value.toString();  
          int pos = line.indexOf("\t");

          if (pos != -1) {          	 
        	  newKey.set(line.substring(0, pos));
        	  newVal.set(line.substring(pos + 1));
        	  context.write(newKey,  newVal);
          }  
      }  
  }
    
  public static class TweetReducer2
  extends Reducer<Text, Text, Text, Text> {
	  private Text newVal = new Text();
	  private HashMap<String, Integer> _subnetOutLinkCountMap = new HashMap<String, Integer>();
	  private HashMap<String, Integer> _subnetInLinkCountMap = new HashMap<String, Integer>();
	  
	  public double[] getMeanAndVar(HashMap<String, Integer> map) {
		  double []res = new double[2];
		  long num = map.size();
		  
		  double mean = 0.0;	
		  for (Entry<String, Integer> entry: map.entrySet()) {
			  mean += entry.getValue();
		  }
		  mean /= num;
		  
		  double var = 0.0;
		  for (Entry<String, Integer> entry: map.entrySet()) {
			  double diff = entry.getValue() - mean;
			  var += diff * diff;
		  }
		  var /= num;
		  
		  res[0] = mean;
		  res[1] = var;
		  return res;
	  }
	  
	  public void reduce(Text key, Iterable<Text> values,  
	            Context context) throws IOException, InterruptedException {            
	      
		  _subnetOutLinkCountMap.clear();
		  _subnetInLinkCountMap.clear();
		  
		  long count = 0;     	  		  
		  for (Text val : values) {
			  String s = val.toString();
			  String [] pair = s.split("\t");
			  // We assume that there are all unique links.
			  if (!_subnetOutLinkCountMap.containsKey(pair[0])){
				  _subnetOutLinkCountMap.put(pair[0], 1);
			  }
			  else {
				  _subnetOutLinkCountMap.put(pair[0], 
						  _subnetOutLinkCountMap.get(pair[0]) + 1);
			  }
			  
			  if (!_subnetInLinkCountMap.containsKey(pair[1])){
				  _subnetInLinkCountMap.put(pair[1], 1);
			  }
			  else {
				  _subnetInLinkCountMap.put(pair[1], 
						  _subnetInLinkCountMap.get(pair[1]) + 1);
			  }
			  
			  count ++;
		  }
		  
		  double [] outRes = getMeanAndVar(_subnetOutLinkCountMap);
		  double [] inRes = getMeanAndVar(_subnetInLinkCountMap);
		  
		  newVal.set(new Long(_subnetOutLinkCountMap.size()).toString() 				
				  + "\t" + new Double(outRes[0]).toString() 
				  + "\t" + new Double(outRes[1]).toString()
				  + "\t" + new Long(_subnetInLinkCountMap.size()).toString() 		
				  + "\t" + new Double(inRes[0]).toString() 
				  + "\t" + new Double(inRes[1]).toString());
		  
		  context.write(key, newVal);
			  
	    }     
	}
  
  public static boolean MR1(String[] otherArgs, Configuration conf) throws Exception {
	  Job job = new Job(conf, "internal outlink subnet extractor");
	    job.setJarByClass(InternalInLinkSubnetExtractor.class);//主类
	    job.setMapperClass(TweetMapper.class);//mapper
	    job.setReducerClass(TweetReducer.class);//reducer
	    
	    // 分区函数  
	    job.setPartitionerClass(FirstPartitioner.class);  
	    // 分组函数  
	    job.setGroupingComparatorClass(GroupingComparator.class);  
	    
	    // map 输出Key的类型  
	    job.setMapOutputKeyClass(StrLongPair.class);  
	    // map输出Value的类型  
	    job.setMapOutputValueClass(Text.class);  
	    // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
	    job.setOutputKeyClass(Text.class);  
	    // reduce输出Value的类型  
	    job.setOutputValueClass(Text.class);  
	      
	    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。  
	    job.setInputFormatClass(TextInputFormat.class);  
	    // 提供一个RecordWriter的实现，负责数据输出。  
	    job.setOutputFormatClass(TextOutputFormat.class);             
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_temp"));//文件输出	      
	    
	    return job.waitForCompletion(true);	    
  }
  
  @SuppressWarnings("deprecation")
public static boolean MR2(String[] otherArgs, Configuration conf) throws Exception{
	  
	  Job job = new Job(conf, "internal inlink subnet extractor");
	    job.setJarByClass(InternalInLinkSubnetExtractor.class);//主类
	    
	    job.setMapperClass(TweetMapper2.class);//mapper
	    job.setReducerClass(TweetReducer2.class);//reducer	    
	    
	    // map 输出Key的类型  
	    job.setMapOutputKeyClass(Text.class);  
	    // map输出Value的类型  
	    job.setMapOutputValueClass(Text.class);  
	    // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
	    job.setOutputKeyClass(Text.class);  
	    // reduce输出Value的类型  
	    job.setOutputValueClass(Text.class);  
	      
	    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。  
	    job.setInputFormatClass(TextInputFormat.class);  
	    // 提供一个RecordWriter的实现，负责数据输出。  
	    job.setOutputFormatClass(TextOutputFormat.class);             
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "_temp"));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
	    
	    boolean res = job.waitForCompletion(true);
	    
	    if (res) {
	    	//remove tmp dict dir
	    	FileSystem hdfs = FileSystem.get(conf);
	    	//hdfs.delete(new Path(otherArgs[3] + "_temp"));
	    }
	    
	    return res;
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
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
		 
    if (otherArgs.length != 2) {
      System.err.println("Usage: " + className + " <indir> <outdir>");
      System.exit(2);
    }
    
    System.out.println("MR1 ...");
    if (!MR1(otherArgs, conf)) {
    	System.exit(1);
    }
    System.out.println("MR2 ...");
    if (!MR2(otherArgs, conf)) {
    	System.exit(1);
    }
  }
}



