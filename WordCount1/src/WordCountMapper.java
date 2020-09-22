import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 1���Զ����࣬�̳�Mapper
 * 2�� ����Mapper�����������KEY-VALUE������<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
 * 3, 
 * @Description 
 * @author shkstart  Email:shkstart@126.com
 * @version 
 * @date 2020��9��22������11:31:00
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	/**
	 * key : ��ǰ�еĿ�ʼλ���������ļ��е�ƫ����
	 * value ��ǰһ�е���
	 * Contest ��ǰmapper����Ļ�������ͨ��������������������
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
