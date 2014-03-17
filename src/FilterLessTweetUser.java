
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class FilterLessTweetUser {

	private static final String MIN_TWEET_NUM = "min.tweet.num";
	
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
       extends Mapper<Object, Text, StrLongPair, Text>{
	  private final StrLongPair newKey = new StrLongPair();  
      private final Text newVal = new Text();  
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
            	  newKey.set(first, second);  
                  newVal.set(second.toString() + "\t" + rest);  
                  context.write(newKey, newVal);  
              }                            
          }  
      }  
  }
  
  public static class TweetReducer
       extends Reducer<StrLongPair, Text, Text, Text> {
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private Configuration _conf = null;
	  long _minTweetNum;
	  private ArrayList<String> _tmpArr = new ArrayList<String>();
	  
	    @Override
		protected void setup(Context context
	            ) throws IOException, InterruptedException {
			super.setup(context);
			_conf = context.getConfiguration();
			_minTweetNum = Long.parseLong(_conf.get(MIN_TWEET_NUM));			

		}	        
	  
      public void reduce(StrLongPair key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {                       	  
    	  
    	  long count = 0;
    	  //Here we assume that all tweets of one user are grouped together

    	  _tmpArr.clear();
    	  for (Text val : values) {
        	  String s = val.toString();        	  
        	  _tmpArr.add(s);
    		  count ++;
          }  
    	  if (count < _minTweetNum) {
    		  return;
    	  }
    	  
    	  for (String s : _tmpArr) {
        	  int pos = s.indexOf("\t");
        	  if (pos != -1) {
        		  newKey.set(s.substring(0, pos) + "\t" + key.getFirst());
        		  newVal.set(s.substring(pos + 1));
        		  context.write(newKey, newVal);
        	  }
    	  }
      }     
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    /**
     * 这里必须有输入/输出
     */
    if (otherArgs.length != 3) {
      System.err.println("Usage: FilterLessTweetUser <min-tweet-num> <indir> <outdir>");
      System.exit(2);
    }
    Job job = new Job(conf, "count user");
    
    //job.setNumReduceTasks(1);
    
    job.setJarByClass(FilterLessTweetUser.class);//主类
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
    
    job.getConfiguration().set(MIN_TWEET_NUM, otherArgs[0]);
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));//文件输入
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));//文件输出
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}


