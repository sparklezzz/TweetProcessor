package merge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SingleFeatureMerger {
  		
	
  public static class TweetMapper
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
        	  context.write(newKey, newVal);              
          }  
      }  
  }
  
  public static class TweetReducer
    extends Reducer<Text, Text, Text, Text> {
	  
    public void reduce(Text key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {            
            	  
    	  for (Text val : values) {
    		  context.write(key, val);
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
      System.err.println("Usage: " + className + " <indir> <outdir>");
      System.exit(2);
    }        
		     
    Job job = new Job(conf, className);
    job.setNumReduceTasks(1);
    
    job.setJarByClass(SingleFeatureMerger.class);//主类
    job.setMapperClass(TweetMapper.class);//mapper
    job.setReducerClass(TweetReducer.class);//reducer
    
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
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}


