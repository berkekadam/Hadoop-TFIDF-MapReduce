package pc;

import java.io.IOException; 
import java.util.HashMap; 
import java.util.Map; 
import java.util.Map.Entry; 
 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 
public class Reducer2 extends Reducer<Text, Text, Text, Text> { 
	
 private static Text outputKey = new Text(); 
 private static Text outputValue = new Text(); 
 
 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
  int Ncounter = 0; 
  
  Map<String, Integer> tempMap= new HashMap<String, Integer>(); // for writing the output file
 
  for (Text val : values) { 
   String[] wordCounter = val.toString().split("="); // splitted "word=n"
   tempMap.put(wordCounter[0], Integer.valueOf(wordCounter[1])); // word (as Key) and count (n) (as Value) added to tempMap
   Ncounter += Integer.parseInt(wordCounter[1]); // get sum of all n's for a document
  } // every docname got its N.

  for (Entry<String, Integer> entry : tempMap.entrySet()) { //iterate over tempMap
	  outputKey.set(entry.getKey() + "@" + key.toString()); // word @ docname  -- docname was in Text key coming from mapper
	  outputValue.set(entry.getValue() + "/" + Ncounter); // n / N 
   context.write(outputKey, outputValue); 
  } 
 }
}