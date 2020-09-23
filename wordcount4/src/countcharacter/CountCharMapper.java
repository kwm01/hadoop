package countcharacter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountCharMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable ikey, Text ivalue, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		char[] charArray = line.toCharArray();
		for(char c: charArray){
			context.write(new Text(c+""), new IntWritable(1));
		}
	}

}
