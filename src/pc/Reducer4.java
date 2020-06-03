package pc;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer4 extends Reducer<Text, Text, Text, Text> { 
	private static DecimalFormat formatter = new DecimalFormat("#0.00000");
	 private static Text outputKey = new Text(); 
	 private static Text finalOutput = new Text(); 
//	 DecimalFormat formatter = new DecimalFormat("##.####");  
	 
 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	 // Keys are word@doc, Values are n/N,m
	 
	 for (Text val : values) { 
		   String[] splitter = val.toString().split(","); 
		   String[] termFreq = splitter[0].split("/");
		   double tf = Double.parseDouble(termFreq[0]) / Double.parseDouble(termFreq[1]); 
		   double idf = (1 + (Math.log(19/Double.parseDouble(splitter[1]))));
		   double tfidf = tf * idf;
//		   finalOutput.set("("+formatter.format(tfidf)+")");
		   finalOutput.set("("+formatter.format(tfidf)+")");
		   context.write(key, finalOutput); 
		  }  
 }

}
