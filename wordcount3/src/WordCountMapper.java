import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 1，自定义类，继承Mapper
 * 2， 定义Mapper的输入输出的KEY-VALUE的类型<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
 * 3, 
 * @Description 
 * @author shkstart  Email:shkstart@126.com
 * @version 
 * @date 2020年9月22日上午11:31:00
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	/**
	 * key : 当前行的开始位置在整个文件中的偏移量
	 * value 当前一行的容
	 * Contest 当前mapper组件的环境对象，通过环境对象向外输出结果
	 */
	@Override
	public void map(LongWritable ikey, Text ivalue, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		String line = ivalue.toString();
		String[] arr = line.split(" ");
		for(String str: arr){
			context.write(new Text(str), new LongWritable(1));
		}
	}
}
