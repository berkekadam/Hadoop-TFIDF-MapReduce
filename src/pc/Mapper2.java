package pc;
import java.io.IOException; 

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> { 
 
 private static Text docAsKey = new Text(); 
 private static Text restAsValue = new Text(); 
 
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
 
	 // we have an input in format "word@docname \t n"
	 // reducer1 defaultly puts tab in between outputkey and value
	 // now we'll calculate a document's total word count as N. So our key should be docname.
	 
  String[] getWordCount = value.toString().split("\t"); 
  	// getWordCount[0] = word@docname
  	//getWordCount[1] = n
  String[] getWordandDoc = getWordCount[0].split("@"); 
  // getWordandDoc [0] = word
  //getWordandDoc [1] = docname
 
  docAsKey.set(getWordandDoc[1]); // docname is the key
  restAsValue.set(getWordandDoc[0] + "=" + getWordCount[1]); // word = n
  context.write(docAsKey, restAsValue); 
 } 
}
