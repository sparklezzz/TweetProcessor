
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class TweetFilterWithoutTweetDict {	
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
	  private PluralRemovalStemmer _stemmer = new PluralRemovalStemmer();
	  private StringBuffer _wordBuf = new StringBuffer();
      
      String parseContent(String body) {
    	  StringTokenizer tokenizer = new StringTokenizer(body);
    	  
    	  //filter tweet with less then 3 words
    	  if (tokenizer.countTokens() < 3) {
    		  return "";
    	  }
    	  
    	  StringBuffer contentBuf = new StringBuffer();
    	  
    	  String word, newWord;
    	  while (tokenizer.hasMoreTokens()) {
    		  word = tokenizer.nextToken();
    		  
    		  if (word.charAt(0) == '@') {
    			  continue;
    		  }
    		  
    		  //only lowercase words with hashtag
    		  if (word.charAt(0) == '#') {
    			  if (word.length() == 1) {
    				  continue;
    			  }
    			  word = word.toLowerCase();

    			  char c = word.charAt(word.length() - 1);
    			  if ((c>='a' && c<='z') || (c>='A' && c<='Z') || (c>='0' && c<='9')) {
    				  contentBuf.append(word + " ");
    			  }
    			  else if (word.length() > 2){
    				  contentBuf.append(word.substring(0, word.length() - 1) + " ");
    			  }    			 
    		  }
    		  else {
	    		  word = word.toLowerCase();
	    		  
	    		  int pos1;
	    		  //filter url
	    		  if (word.startsWith("http://")) {
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".com")) > 0) {	// suffix appear in the non-first pos
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".gov")) > 0){	    			
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".org")) > 0){	    			
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".net")) > 0){	    			
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".edu")) > 0){	    			
	    			  continue;
	    		  }
	    		  else if ((pos1 = word.indexOf(".cn")) > 0){	    			
	    			  continue;
	    		  }
	    		  
	    		  //remove 
	    		  _wordBuf.delete(0, _wordBuf.length());
	    		  for (int i=0; i<word.length(); i++) {
	    			  char c = word.charAt(i);    			  
	    			  if ((c>='a' && c<='z') || (c>='A' && c<='Z')) {
	    				  _wordBuf.append(c);
	    			  }
	    		  }
	    		  
	    		  //remove plural, -ed, -ing
	    		  _stemmer.set(_wordBuf);
	    		  _stemmer.stem();
	    		  newWord = _stemmer.toString();
	    		  
	    		  if (newWord.length() < 3) {
	    			  continue;
	    		  }
	    		  
	    		  contentBuf.append(newWord + " ");
    		  }
    	  }
    	  
    	  if (contentBuf.length() > 1) {
    		  //remove last blank
    		  return contentBuf.substring(0, contentBuf.length() - 1);
    	  }
    	  else {
    		  return contentBuf.toString();
    	  }
      }      
      
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
            	  
            	  String contentParsed = parseContent(rest);
            	  
            	  if (contentParsed == null || contentParsed.isEmpty()) {
            		  return;
            	  }
            	  
                  newVal.set(second.toString() + "\t" + contentParsed);  
                  context.write(newKey, newVal);  
              }                            
          }  
      }  
  }
  
  public static class TweetReducer
       extends Reducer<StrLongPair, Text, Text, Text> {
	  private final Text newKey = new Text();  
	  private final Text newVal = new Text();
	  
      public void reduce(StrLongPair key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {            
          
    	  for (Text val : values) {
        	  String s = val.toString();
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
    job.setJarByClass(TweetFilterWithoutTweetDict.class);//主类
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
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出 
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}



