package pc;
import java.io.IOException; 

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 
public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> { 

	private static IntWritable result = new IntWritable(); 

   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException { 

		int sum = 0; 
		for (IntWritable val : values) { // summation is similar to WordCount example
		 sum += val.get(); 
		} 
		result.set(sum); 
		context.write(key, result); 
		} 
}