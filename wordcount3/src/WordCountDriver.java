import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
//		Job job = Job.getInstance(conf, "WordCount_kang");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(WordCountMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(WordCountReduce.class);

		// TODO: specify output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.211.128:9000/words.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.211.128:9000/result"));

		job.waitForCompletion(true);
	}

}
