package pc;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private StringBuilder keyBuilder = new StringBuilder(); 
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// mapper get document lines as Text typed value. mapper, maps its input to key-value pairs.
		// we'll make (word-intWritable) mapping as similar to WordCount example with some preprocessing.
		
		String rawStr = value.toString();
		rawStr = rawStr.replaceAll("[^a-zA-ZçğıöşüÇĞİÖŞÜ0-9]+]", "");
		rawStr = rawStr.replaceAll("\\p{Punct}", "");
		// there are some apostrophes not be removed. 
		// same problem was with my NLP course's JAVA project. can't remove all punctuation although tried two types of removing.
		
		ArrayList<String> stopwords = new ArrayList<>();
		String encoding = "UTF-16";
		FileInputStream fin = new FileInputStream("/home/hduser/Downloads/stopwordslist.txt");
		BufferedReader in = new BufferedReader(new InputStreamReader(fin, encoding));
	    String line = in.readLine();
	    while (line != null) {
	      stopwords.add(line);
	      line = in.readLine();
	    }
	    in.close();
		
		// document lines converted to string. we have a format like 
		// '$PYTWEET$\t' + tweet_scr_user + '\t' + tweet_id + '\t' + tweet_text + ‘\n’
		// we only need tweet_text		
		String [] splitted = null;
		splitted = rawStr.split("\t");
		String str="";	// tweet_text will be collected here
		for (int i = 3;i<splitted.length;i=i+4) {
				str += splitted[i].toString();
				if (i+4>=splitted.length) break;
		}
				
		String [] words = str.split(" ");
		
		
		for (String word : words) {
			if (word.length()>2 && Character.isLetterOrDigit(word.charAt(0)) && !(stopwords.contains(word))) {
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //get file name
			Text mapperKey = new Text(word.toLowerCase().trim());
			IntWritable one = new IntWritable(1);
			keyBuilder.delete(0, keyBuilder.length()); // new valuebuilder each time 
			keyBuilder.append(mapperKey); 
			keyBuilder.append("@"); 
			keyBuilder.append(fileName); // format is like "word @ doc"
			mapperKey.set(keyBuilder.toString()); 
			context.write(mapperKey, one);
			}
		}
		
	}
	}