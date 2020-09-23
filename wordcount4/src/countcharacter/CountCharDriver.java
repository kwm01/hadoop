package countcharacter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountCharDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CharCount");
		job.setJarByClass(countcharacter.CountCharDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(CountCharMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(CountCharReducer.class);

		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.211.128:9000/train/char/characters.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.211.128:9000/result/charcount"));

		if (!job.waitForCompletion(true))
			return;
	}

}
