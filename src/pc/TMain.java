package pc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TMain {

	
		public static void main(String args[]) throws Exception {
			Configuration conf = new Configuration();

			Job job1 = Job.getInstance(conf, "Task#1 Word Frequency in Doc"); // Create first task's job
			job1.setJarByClass(TMain.class);
			job1.setMapperClass(Mapper1.class);
			job1.setCombinerClass(Reducer1.class);
			job1.setReducerClass(Reducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));	//get input file from hdfs /input/localFS 
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));	//define output path for Task1
			// when running the .jar from Terminal, 4 output paths should be written for 4 tasks each
			job1.waitForCompletion(true); // Wait for first job's completion to proceeding second
			
			Configuration conf2 = new Configuration();  // Create second task's job
			Job job2 = Job.getInstance(conf2, "Task#2 Word Counts For Docs");
			job2.setJarByClass(TMain.class);
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job2, new Path(args[1])); // input of 2nd job is 1st job's output
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			job2.waitForCompletion(true);	
			
			Configuration conf3 = new Configuration();  // Create third task's job
			Job job3 = Job.getInstance(conf3, "Task#3 Word Counts For Docs");
			job3.setJarByClass(TMain.class);
			job3.setMapperClass(Mapper3.class);
			job3.setReducerClass(Reducer3.class);
		    job3.setOutputKeyClass(Text.class);
		    job3.setOutputValueClass(Text.class); 
		    FileInputFormat.addInputPath(job3, new Path(args[2]));
			FileOutputFormat.setOutputPath(job3, new Path(args[3]));	
			job3.waitForCompletion(true);
			
			Configuration conf4 = new Configuration();  // Create fourth task's job
			Job job4 = Job.getInstance(conf4, "Task#4 TFIDF");
			job4.setJarByClass(TMain.class);
			job4.setMapperClass(Mapper4.class);
			job4.setReducerClass(Reducer4.class);
		    job4.setOutputKeyClass(Text.class);
		    job4.setOutputValueClass(Text.class); 
		    FileInputFormat.addInputPath(job4, new Path(args[3]));
			FileOutputFormat.setOutputPath(job4, new Path(args[4]));	
			job4.waitForCompletion(true);
			System.exit(job4.waitForCompletion(true) ? 0 : 1);
			
	}

}
