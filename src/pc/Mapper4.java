package pc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper4 extends Mapper <LongWritable, Text, Text, Text> {
	private static Text mapperKey = new Text(); 
	private static Text mapperValue = new Text(); 
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
  
	// Now we have an input format like "word@docname TAB n/N,m"
	  
	  String[] tabSplit = value.toString().split("\t");
	  
	  mapperKey.set(tabSplit[0]); 
	  mapperValue.set(tabSplit[1]); 
	  context.write(mapperKey, mapperValue); 
	  
  }
}
