import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StateWords2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
      
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
  
  // Job 1 and 2 Reducer
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
	  extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context
		               ) throws IOException, InterruptedException {
			
		 StringTokenizer itrLine = new StringTokenizer(value.toString(), "\n");
		 
		 while (itrLine.hasMoreTokens()) {
			 String[] lineSplit = itrLine.nextToken().split("\\s+");
			 Text stateCount = new Text(lineSplit[1] + " " +  lineSplit[2]);
			 context.write(new Text(lineSplit[0]), stateCount);
		 }
	}
} 
  // Job 3 Reducer
  public static class MaxReducer
	  extends Reducer<Text,Text,Text,Text> {
	  		
		public void reduce(Text key, Iterable<Text> values,
		                  Context context
		                  ) throws IOException, InterruptedException {
			
	     
	     
	     ArrayList<String> statesList = new ArrayList<String>();
		 for (Text val : values) {
			 statesList.add(val.toString());
		 }
		 
		 Collections.sort(statesList, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				String[] array1 = o1.split("\\s+");
				String[] array2 = o2.split("\\s+");
				return Integer.parseInt(array2[1]) - Integer.parseInt(array1[1]);
			}
			 
		 });
		 
		 String result = StringUtils.join(statesList.subList(0, 3), ", ");

		 Text sortedStateCounts  = new Text(result);
		 
		 context.write(key, sortedStateCounts);
		 
	}
} 
  

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
    
    //////////////////////////////////////////////////////////////
    job2.waitForCompletion(true);
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "word maps to array of states");
    job3.setJarByClass(StateWords2.class);
    job3.setMapperClass(WordStateCountMapper2.class); // Combiner not used because mapper outputs different type 
    job3.setReducerClass(MaxReducer.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(args[1]));
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
    //////////////////////////////////////////////////////////////
    
  }
}