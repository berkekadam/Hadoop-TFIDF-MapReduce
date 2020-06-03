package pc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> { 
	private static Text mapperKey = new Text(); 
	private static Text mapperValue = new Text(); 
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 

	 // we have an input format word@docname   \t	n/N. our output should be ((word, docname), (n, N, m))
	 // m is the number of documents with the term "word" in it. so word should be sent to reducer3 as Key.
	  
	 String[] tabSplit = value.toString().split("\t"); 
	 String[] getWordDoc = tabSplit[0].split("@"); 
	
	 mapperKey.set(getWordDoc[0]); // word is in here
	 mapperValue.set(getWordDoc[1] + "=" + tabSplit[1]); // "docname = n/N"
	 context.write(mapperKey, mapperValue); 
  } 
}