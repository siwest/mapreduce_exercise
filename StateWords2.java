import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateWords2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      
      List<Text> list = new ArrayList<Text>();
      list.add(new Text("education"));
      list.add(new Text("politics"));
      list.add(new Text("sports"));
      list.add(new Text("agriculture"));
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
  	  	if (list.contains(word)) {
  	  		context.write(new Text(word.toString()  + " " + fileName), one);
  	  	}
      }
    }
  }
  
  // Job 1
  /* Combines data from states files to one output*/
  public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      
    }
  }
  
  
  
  // Job 2 Mapper
  public static class WordStateCountMapper
	  extends Mapper<Object, Text, Text, IntWritable>{
	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context
		               ) throws IOException, InterruptedException {
			
		 StringTokenizer itrLine = new StringTokenizer(value.toString(), "\n");
		 
		 // Take each line and interpret as word[0] = word, word[1] = state, word[2] = count
		 // then context.write(word[0], word[2] aka count)
		 
		 while (itrLine.hasMoreTokens()) {
			 String[] lineSplit = itrLine.nextToken().split("\\s+");
			 int count = Integer.parseInt(lineSplit[2]);
			 context.write(new Text(lineSplit[0]), new IntWritable(count));
		 }
	}
}
  
  
  
  // Job 3 Mapper
  public static class WordStateCountMapper2
	  extends Mapper<Object, Text, Text, IntWritable>{
	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context
		               ) throws IOException, InterruptedException {
			
		 StringTokenizer itrLine = new StringTokenizer(value.toString(), "\n");
		 
		 // Take each line and interpret as word[0] = word, word[1] = state, word[2] = count
		 // then context.write(word[0], word[2] aka count)
		 
		 while (itrLine.hasMoreTokens()) {
			 String[] lineSplit = itrLine.nextToken().split("\\s+");
			 int count = Integer.parseInt(lineSplit[2]);
			 context.write(new Text(lineSplit[0]), new IntWritable(count));
		 }
	}
} 
  
  
  
  
  /* Totals words for each state */
 /* public static class IntSumReducer
  	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		private Text tmpWord = new Text("");
		private Text tmpState = new Text("");
	    private int tmpFrequency = 0;
		
	    // Maps word: state, count
	    HashMap<String, HashMap<String, Integer> > mapWordStates = new HashMap <String, HashMap<String, Integer>>();
	    
		
		public void reduce(Text key, Iterable<IntWritable> values,
		                  Context context
		                  ) throws IOException, InterruptedException {		 
		 
		// Collect top 3 state counts by word
		 for (IntWritable val : values) {
			 // stateWords composite key is "State Word". 
			 // 	stateWords[0] = state
			 // 	stateWords[1] = word 
			  
			 String[] stateWords = key.toString().split(" ");
			 HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
			 tempMap.put(stateWords[0], val.get());
			 
			 // If Word is already in map of words
			 if (mapWordStates.containsValue(stateWords[1])) {
				 
				 // Check if state already has 3 entries
				 if (mapWordStates.get(stateWords[1]).size() == 3) {
					 // Iterate over all 3 entries and remove min if smaller than tempMap value					 
					 Iterator it = mapWordStates.get(stateWords[1]).entrySet().iterator();			 
					 while (it.hasNext()) {	 
						 HashMap.Entry pair = (HashMap.Entry) it.next();
						 if (val.get() > (int) pair.getValue()) {
							 mapWordStates.get(stateWords[1]).remove(pair.getKey());
							 
						 }
					 }
					 mapWordStates.put(stateWords[1], tempMap);
				 }
			
			 } else {
				 mapWordStates.put(stateWords[1], tempMap);
			 }	 
		 }
		 
		 
		 
		 context.write(key, result);
		 
		}
}*/
  


  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "word-state count");
    job1.setJarByClass(StateWords2.class);
    job1.setMapperClass(TokenizerMapper.class);
    job1.setCombinerClass(IntSumCombiner.class);
    job1.setReducerClass(IntSumCombiner.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    
    ///////////////////////////////////////////////////////////////
    job1.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "sum words over states");
    job2.setJarByClass(StateWords2.class);
    job2.setMapperClass(WordStateCountMapper.class);
    job2.setCombinerClass(IntSumCombiner.class);
    job2.setReducerClass(IntSumCombiner.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
    //////////////////////////////////////////////////////////////
    
  }
}