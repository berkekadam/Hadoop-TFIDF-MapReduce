package pc;
import java.io.IOException; 
import java.text.DecimalFormat; 
import java.util.HashMap; 
import java.util.Map; 
import java.util.Map.Entry; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer3 extends Reducer<Text, Text, Text, Text> { 
	 private static Text outputKey = new Text(); 
	 private static Text outputValue = new Text(); 
	 
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
		
	  int m = 0;  // m as number of documents with the term "word" in it
	  
	  // again the algorithm grom Reducer2 is used.
	  
	  Map<String, String> tempMap = new HashMap<String, String>(); 
	  for (Text val : values) { 
	   String[] splitter = val.toString().split("="); 
	   m++; 
	   tempMap.put(splitter[0], splitter[1]); 
	   				// DOCNAME , n/N
	  } 
	 
	  for (Entry<String, String> entry : tempMap.entrySet()) { 
	   String[] wordCountsforDocs = entry.getValue().split("/"); 
	 
	   outputKey.set(key + "@" + entry.getKey()); // match the word with its docname (key from mapper3, entry.getkey is splitter[0]
	   outputValue.set(wordCountsforDocs[0]  + "/" + wordCountsforDocs[1] + "," + m ); 
	   context.write(outputKey, outputValue); 
	  } 
	 } 
	}
