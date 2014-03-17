
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import util.PluralRemovalStemmer;

public class TweetDictExtractor {
  private static final String MIN_CF = "min.cf";
  private static final String STOPWORD_DICT = "stopword.dict";
    
  public static class TweetMapper
       extends Mapper<Object, Text, Text, LongWritable>{
	  private final Text newKey = new Text();  
      private PluralRemovalStemmer stemmer = new PluralRemovalStemmer();
	  private StringBuffer wordBuf = new StringBuffer();
      private Configuration _conf = null;
      private HashSet<String> _stopwordDict = new HashSet<String>();
	  
	  @Override
	  protected void setup(Context context
	            ) throws IOException, InterruptedException {
			super.setup(context);
			_conf = context.getConfiguration();

			String tweetDictFileName = _conf.get(STOPWORD_DICT);			

			//get local cached stopword dict
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(_conf);
			String tweetDictCachePath = "";
			for (int i=0; i<cachePaths.length; i++) {
				if (cachePaths[i].getName().equals(tweetDictFileName)) {
					tweetDictCachePath = cachePaths[i].toString();
					break;
				}
			}
			
			//load tweet dict
			BufferedReader br = new BufferedReader(new FileReader(tweetDictCachePath));
			_stopwordDict.clear(); 
			String line;
			while ((line = br.readLine()) != null) {
				if (!line.isEmpty()) {
					int pos = line.indexOf("\t");
					if (pos == -1)
						_stopwordDict.add(line);
					else
						_stopwordDict.add(line.substring(0, pos));
				}
			}			
			br.close();			
	  }
      
      
      void parseContent(String body, Context context) 
    		  throws IOException, InterruptedException {
    	  StringTokenizer tokenizer = new StringTokenizer(body);
    	  
    	  //filter tweet with less then 3 words
    	  if (tokenizer.countTokens() < 3) {
    		  return;
    	  }
    	  
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
        			  newKey.set(word);
            		  context.write(newKey, new LongWritable(1));
    			  }
    			  else if (word.length() > 2){
    				  word = word.substring(0, word.length() - 1);
        			  newKey.set(word);
            		  context.write(newKey, new LongWritable(1));
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
	    		  wordBuf.delete(0, wordBuf.length());
	    		  for (int i=0; i<word.length(); i++) {
	    			  char c = word.charAt(i);    			  
	    			  if ((c>='a' && c<='z') || (c>='A' && c<='Z')) {
	    				  wordBuf.append(c);
	    			  }
	    		  }
	    		  
	    		  //remove plural, -ed, -ing
	    		  stemmer.set(wordBuf);
	    		  stemmer.stem();
	    		  newWord = stemmer.toString();
	    		  
	    		  if (newWord.length() < 3) {
	    			  continue;
	    		  }
	    		  
	    		  if (_stopwordDict.contains(newWord)) {
	    			  continue;
	    		  }
	    		  
	    		  newKey.set(newWord);
	    		  context.write(newKey, new LongWritable(1));
    		  }
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
            	  
            	  //deal with actual content            	              	            	  
            	  parseContent (rest, context);
              }                            
          }  
      }  
  }
  
  public static class TweetReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {
	  
    public void reduce(Text key, Iterable<LongWritable> values,  
              Context context) throws IOException, InterruptedException {            
          
    	  long sum = 0;    	  
    	  for (LongWritable val : values) {
    		  sum += val.get();
          }  
		  context.write(key, new LongWritable(sum));
      }     
  }

  
  private static class LongWritableDecreasingComparator extends LongWritable.Comparator {  
      public int compare(WritableComparable a, WritableComparable b) {  
        return -super.compare(a, b);  
      }  
        
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {  
          return -super.compare(b1, s1, l1, b2, s2, l2);  
      }  
  }  
  
  
  public static class TweetMapper2
  extends Mapper<Object, Text, LongWritable, Text>{
	  private final Text newVal = new Text();  
  
	  public void map(Object key, Text value, Context context)  
              throws IOException, InterruptedException {  
    	  String line = value.toString();  
          int pos = line.indexOf("\t");

          if (pos != -1) {          	 
        	  newVal.set(line.substring(0, pos));
        	  
        	  context.write(
        			  new LongWritable(Long.parseLong(line.substring(pos + 1))), 
        			  newVal);
          }  
      }  
  }
  
  
  public static class TweetReducer2
  extends Reducer<LongWritable, Text, Text, LongWritable> {
	  private long _min_cf = 1;
	  private Configuration _conf = null;
	  
	  @Override
	  protected void setup(Context context
	            ) throws IOException, InterruptedException {
			super.setup(context);
			_conf = context.getConfiguration();
			_min_cf = _conf.getLong(MIN_CF, 1);
	  }
	  
	  public void reduce(LongWritable key, Iterable<Text> values,  
	            Context context) throws IOException, InterruptedException {            
	           	  
	  	 boolean low = (key.get() < _min_cf);
		  
		  for (Text val : values) {
			  if (low && (val.toString().charAt(0) != '#'))
			  	continue;
			  context.write(val, key);
	       }  
			  
	    }     
	}
  
  public static boolean extract(String[] otherArgs, Configuration conf) throws Exception {
	  Job job = new Job(conf, "tweet dict extractor");
	    job.setJarByClass(TweetDictExtractor.class);//主类
	    job.setMapperClass(TweetMapper.class);//mapper
	    job.setReducerClass(TweetReducer.class);//reducer
	    job.setCombinerClass(TweetReducer.class);//combiner
	      
	    // map 输出Key的类型  
	    job.setMapOutputKeyClass(Text.class);  
	    // map输出Value的类型  
	    job.setMapOutputValueClass(LongWritable.class);  
	    // reduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat  
	    job.setOutputKeyClass(Text.class);  
	    // reduce输出Value的类型  
	    job.setOutputValueClass(LongWritable.class);  
	      
	    // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。  
	    job.setInputFormatClass(TextInputFormat.class);  
	    // 提供一个RecordWriter的实现，负责数据输出。  
	    job.setOutputFormatClass(TextOutputFormat.class);             
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3] + "_temp"));//文件输出
	    
	    /*
		 * Add tweet dict to distributed cache
		 */		
		Configuration jconf = job.getConfiguration();
		String userProgOnHDFS = otherArgs[1];
		Path userProgPath = new Path(userProgOnHDFS);
		String filename = userProgPath.getName();
		jconf.set(STOPWORD_DICT, filename);
		URI userProgUri = new URI(userProgOnHDFS + "#_stopwordDict");
		DistributedCache.addCacheFile(userProgUri, jconf);
		DistributedCache.createSymlink(jconf);    
	    
	    return job.waitForCompletion(true);	    
  }
  
  @SuppressWarnings("deprecation")
public static boolean sort(String[] otherArgs, Configuration conf) throws Exception{
	  conf.set(MIN_CF, otherArgs[2]);
	  
	  Job job = new Job(conf, "tweet dict extractor");
	    job.setJarByClass(TweetDictExtractor.class);//主类
	    
	    job.setMapperClass(TweetMapper2.class);//mapper
	    job.setReducerClass(TweetReducer2.class);//reducer
	    
	    job.setNumReduceTasks(1);
	    job.setSortComparatorClass(LongWritableDecreasingComparator.class);
	    
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
	    
	    FileInputFormat.addInputPath(job, new Path(otherArgs[3] + "_temp"));//文件输入
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));//文件输出
	    
	    boolean res = job.waitForCompletion(true);
	    
	    if (res) {
	    	//remove tmp dict dir
	    	FileSystem hdfs = FileSystem.get(conf);
	    	hdfs.delete(new Path(otherArgs[3] + "_temp"));
	    }
	    
	    return res;
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    /**
     * 这里必须有输入/输出
     */
    if (otherArgs.length != 4) {
      System.err.println("Usage: TweetDictExtractor <indir> <stopword_dict_on_HDFS> <min_cf> <outdir>");
      System.exit(2);
    }
    
    System.out.println("Extracting dict...");
    if (!extract(otherArgs, conf)) {
    	System.exit(1);
    }
    System.out.println("Sorting dict");
    if (!sort(otherArgs, conf)) {
    	System.exit(1);
    }
  }
}


