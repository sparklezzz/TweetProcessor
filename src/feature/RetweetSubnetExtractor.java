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
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.PluralRemovalStemmer;
import util.TweetParser;

public class RetweetSubnetExtractor {
	
	private static final String USER_RETWEET_LINK_FILE = "user.retweet.link.file";
	
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
          return l.compareTo(r);  
        }  
      }      
  
  public static class TweetMapper
       extends Mapper<Object, Text, Text, Text>{
	  private final Text newKey = new Text(); 
	  private final Text newKey2 = new Text();
	  private final Text newVal = new Text();
      //private final Text newVal = new Text();  
	  private Configuration _conf = null;             
	  private final TweetParser _parser = new TweetParser();
	  
      public void map(Object key, Text value, Context context)  
              throws IOException, InterruptedException {  
    	  String line = value.toString();  
          int pos = line.indexOf("\t");

          if (pos != -1) {          	 
        	  Long second = Long.parseLong(line.substring(0, pos));
              int pos2 = line.indexOf("\t", pos + 1);
              if (pos2 != -1) {
            	  String first = line.substring(pos + 1, pos2);
            	  String rest = line.substring(pos2 + 1);              	  
            	  newKey.set(first);
            	  
            	  String toUser = _parser.GetRetweetUser(rest);
            	  if (toUser != null && !toUser.isEmpty()) {
            		  newKey2.set(toUser);
					  newVal.set(first + "\t" + toUser);
					  context.write(newKey, newVal);
					  context.write(newKey2, newVal);
            	  }                	  
              }                            
          }  
      }  
  }
  
  public static class TweetReducer
       extends Reducer<Text, Text, Text, Text> {
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private Configuration _conf = null;

	  private HashSet<String> _toList = new HashSet<String>();
	  private HashSet<String> _fromList = new HashSet<String>();

	  private HashMap<String, Long> _outDegreeMap = new HashMap<String, Long>();
	  private HashMap<String, Long> _inDegreeMap = new HashMap<String, Long>();

	  @Override
		protected void setup(Context context
	            ) throws IOException, InterruptedException {
			super.setup(context);
			_conf = context.getConfiguration();
			String userFileName = _conf.get(USER_RETWEET_LINK_FILE);			

			//get local cached tweet dict
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(_conf);
			String userFileCachePath = "";
			for (int i=0; i<cachePaths.length; i++) {
				if (cachePaths[i].getName().equals(userFileName)) {
					userFileCachePath = cachePaths[i].toString();
					break;
				}
			}
			
			//load tweet dict
			BufferedReader br = new BufferedReader(new FileReader(userFileCachePath));

			_outDegreeMap.clear();
			_inDegreeMap.clear();
			String line;
			while ((line = br.readLine()) != null) {
				String[] lst = line.split("\t");
				if (lst == null || lst.length < 5)
					continue;
				
				_outDegreeMap.put(lst[0], new Long(lst[2]));
				_inDegreeMap.put(lst[0], new Long(lst[4]));
			}			
			br.close();
		}		  
	  
	  public double[] getMeanAndVar(HashSet<String> set, HashMap<String, Long> map) {
		  double []res = new double[2];
		  long num = set.size();
		  
		  double mean = 0.0;		  
		  ArrayList<Long> tmpArr = new ArrayList<Long>();
		  for (String user : set) {
			  if (map.containsKey(user)) {
				  Long val = map.get(user);
				  mean += val;
				  tmpArr.add(val);
			  }
			  else {
				  tmpArr.add(0L);
			  }
		  }
		  mean /= num;
		  
		  double var = 0.0;
		  for (Long n : tmpArr) {
			  double diff = n - mean;
			  var += diff * diff;
		  }
		  var /= num;
		  
		  res[0] = mean;
		  res[1] = var;
		  return res;
	  }
	  
      public void reduce(Text key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {            
          
    	  if (!_outDegreeMap.containsKey(key.toString()))
    		  return;
    	  
          long uniqueTotalOut = 0;
          long uniqueTotalIn = 0;            	  
    	  
          _fromList.clear();
          _toList.clear();
          
          String from, to;
          for (Text val : values) {
        	  String s = val.toString();
        	  int pos = s.indexOf("\t");
        	  from = s.substring(0, pos);
        	  to = s.substring(pos + 1);
        	  
        	  if (!(_outDegreeMap.containsKey(from) && _inDegreeMap.containsKey(to))) {
        		  continue;
        	  }       		
        	  if (from.equals(to)) {	//ignore self link
        		  continue;
        	  }
        	  else if (from.equals(key.toString())) {
        		  if (_toList.add(to)) {
        			  uniqueTotalOut ++;
        		  }
        	  }
        	  else if (to.equals(key.toString())) {
        		  if (_fromList.add(from)) {
        			  uniqueTotalIn ++;
        		  }
        	  }
          }                      
          
          double[] outSubnetOutStat = getMeanAndVar(_toList, _outDegreeMap);
          double[] outSubnetInStat = getMeanAndVar(_toList, _inDegreeMap);
          double[] inSubnetOutStat = getMeanAndVar(_fromList, _outDegreeMap);
          double[] inSubnetInStat = getMeanAndVar(_fromList, _inDegreeMap);          
    	  
    	  newKey.set(key);
    	  newVal.set(	   new Double(outSubnetOutStat[0]).toString() 
    			  + "\t" + new Double(outSubnetOutStat[1]).toString()
    			  + "\t" + new Double(outSubnetInStat[0]).toString() 
    			  + "\t" + new Double(outSubnetInStat[1]).toString()
    			  + "\t" + new Double(inSubnetOutStat[0]).toString() 
    			  + "\t" + new Double(inSubnetOutStat[1]).toString()
    			  + "\t" + new Double(inSubnetInStat[0]).toString() 
    			  + "\t" + new Double(inSubnetInStat[1]).toString()
    			  	);
    	  context.write(newKey, newVal);
      }     
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
		 
    if (otherArgs.length != 3) { 	
      System.err.println("Usage: " + className + " <hdfs-dict-path (with hdfs prefix)> <hdfs-indir> <hdfs-outdir>");
      System.exit(2);
    }
    Job job = new Job(conf, className);
    job.setJarByClass(RetweetSubnetExtractor.class);//主类
    job.setMapperClass(TweetMapper.class);//mapper
    job.setReducerClass(TweetReducer.class);//reducer
  
    // 分区函数  
    //job.setPartitionerClass(FirstPartitioner.class);  
    // 分组函数  
    //job.setGroupingComparatorClass(GroupingComparator.class);  
      
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
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//文件输入
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));//文件输出 
    
    /*
	 * Add tweet dict to distributed cache
	 */
	Configuration jconf = job.getConfiguration();
	String userProgOnHDFS = otherArgs[0];
	Path userProgPath = new Path(userProgOnHDFS);
	String filename = userProgPath.getName();
	jconf.set(USER_RETWEET_LINK_FILE, filename);
	URI userProgUri = new URI(userProgOnHDFS + "#_userLinkDict");
	DistributedCache.addCacheFile(userProgUri, jconf);
	DistributedCache.createSymlink(jconf);         
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}



