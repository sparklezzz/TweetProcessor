package feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.PluralRemovalStemmer;
import util.TweetParser;

public class WordStatExtractor {	
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
    public static class FirstPartitioner extends Partitioner<StrLongPair,LongWritable>{  
      @Override  
      public int getPartition(StrLongPair key, LongWritable value,   
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
       extends Mapper<Object, Text, StrLongPair, LongWritable>{
	  private final StrLongPair newKey = new StrLongPair();  
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
            	  newKey.set(first, second);              	  
            	  long WordCount = _parser.countWord(rest);            	            	  
                  context.write(newKey, new LongWritable(WordCount));  
              }                            
          }  
      }  
  }
  
  public static class TweetReducer
       extends Reducer<StrLongPair, LongWritable, Text, Text> {
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  private ArrayList<Long> _tmpWCArr = new ArrayList<Long>();
	  
      public void reduce(StrLongPair key, Iterable<LongWritable> values,  
              Context context) throws IOException, InterruptedException {            
          long totalWordCount = 0;
          long totalTweetCount = 0;
    	 
    	  _tmpWCArr.clear();
          
    	  //calculate expectation
          for (LongWritable val : values) {
        	  totalTweetCount ++;
    		  totalWordCount += val.get();
    		  _tmpWCArr.add(val.get());
          }    	  
    	  double avgWord = totalWordCount / (double)totalTweetCount;
    	  
    	  //calculate variance
    	  double variance = 0.0;
    	  for (long wc : _tmpWCArr) {
    		  double diff = wc - avgWord;
    		  variance += diff * diff;
    	  }
    	  variance /= totalTweetCount;
    	  
    	  newKey.set(key.getFirst());
    	  newVal.set(new Long(totalWordCount).toString() + "\t" + new Double(avgWord).toString()
    			  + "\t" + new Double(variance).toString());
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
		 
    if (otherArgs.length != 2) { 	
      System.err.println("Usage: " + className + " <hdfs-indir> <hdfs-outdir>");
      System.exit(2);
    }
    Job job = new Job(conf, className);
    job.setJarByClass(WordStatExtractor.class);//主类
    job.setMapperClass(TweetMapper.class);//mapper
    job.setReducerClass(TweetReducer.class);//reducer
  
    // 分区函数  
    job.setPartitionerClass(FirstPartitioner.class);  
    // 分组函数  
    job.setGroupingComparatorClass(GroupingComparator.class);  
      
    // map 输出Key的类型  
    job.setMapOutputKeyClass(StrLongPair.class);  
    // map输出Value的类型  
    job.setMapOutputValueClass(LongWritable.class);  
    // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
    job.setOutputKeyClass(Text.class);  
    // reduce输出Value的类型  
    job.setOutputValueClass(Text.class);  
      
    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。  
    job.setInputFormatClass(TextInputFormat.class);  
    // 提供一个RecordWriter的实现，负责数据输出。  
    job.setOutputFormatClass(TextOutputFormat.class);             
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出 
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}



