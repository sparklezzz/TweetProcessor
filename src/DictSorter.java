
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

@Deprecated	//integrated in TweetDictExtractor Class
public class DictSorter {
  
	static class ReverseComparator extends WritableComparator {
        private static final LongWritable.Comparator LONG_COMPARATOR = new LongWritable.Comparator();

        public ReverseComparator() {
            super(LongWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return (-1)* LONG_COMPARATOR
			        .compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof LongWritable && b instanceof LongWritable) {
                return (-1)*(((LongWritable) a)
                        .compareTo((LongWritable) b));
            }
            return super.compare(a, b);
        }
    }
	
	
	
  public static class TweetMapper
       extends Mapper<Object, Text, LongWritable, Text>{
	  private final Text newVal = new Text();         
      
      public void map(Object key, Text value, Context context)  
              throws IOException, InterruptedException {  
    	  String line = value.toString();  
          int pos = line.indexOf("\t");

          if (pos != -1) {  
        	  String first = line.substring(0, pos);
        	  Long second = Long.parseLong(line.substring(pos + 1));
        	  
        	  newVal.set(first);
        	  context.write(new LongWritable(second), newVal);              
          }  
      }  
  }
  
  public static class TweetReducer
    extends Reducer<LongWritable, Text, Text, LongWritable> {
	  
    public void reduce(LongWritable key, Iterable<Text> values,  
              Context context) throws IOException, InterruptedException {            
            	  
    	  for (Text val : values) {
    		  context.write(val, key);
          }  
		  
      }     
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    /**
     * 这里必须有输入/输出
     */
    if (otherArgs.length != 2) {
      System.err.println("Usage: DictSorter <indir> <outdir>");
      System.exit(2);
    }    
    
    Job job = new Job(conf, "dict sorter");
    job.setNumReduceTasks(1);
    
    job.setJarByClass(TweetDictExtractor.class);//主类
    job.setMapperClass(TweetMapper.class);//mapper
    job.setReducerClass(TweetReducer.class);//reducer
    
    job.setSortComparatorClass(ReverseComparator.class);
    
    // map 输出Key的类型  
    job.setMapOutputKeyClass(LongWritable.class);  
    // map输出Value的类型  
    job.setMapOutputValueClass(Text.class);  
    // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
    job.setOutputKeyClass(Text.class);  
    // reduce输出Value的类型  
    job.setOutputValueClass(LongWritable.class);  
      
    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。  
    job.setInputFormatClass(TextInputFormat.class);  
    // 提供一个RecordWriter的实现，负责数据输出。  
    job.setOutputFormatClass(TextOutputFormat.class);             
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//文件输出
    System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
  }
}


