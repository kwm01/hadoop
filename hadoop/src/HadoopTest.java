import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import utils.HadoopUtil;

public class HadoopTest {
	@Test
	public void test1(){
		//1,select Configuration of XML 
		Configuration conf = new Configuration();
		//2, connect to hadoop by FileSystem
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRead(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf,HadoopUtil.getUser());
			
			InputStream is = fs.open(new Path("/java/data")); 
			FileOutputStream fos = new FileOutputStream("testRead_data.txt");
			byte[] buffer = new byte[1024];
			int len;
			while((len = is.read(buffer))!= -1){
				fos.write(buffer, 0, len);
			}
			is.close();
			fos.close();
			fs.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test 
	public void testMkdir(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			//to do operation
			fs.mkdirs(new Path("/dirFromAOI"));
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test 
	public void testUpload0(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			//to do operation
			FileInputStream fis = new FileInputStream("testRead_data2.txt");
			FSDataOutputStream fos = fs.create(new Path("/uploadtest/03.txt"));
			IOUtils.copyBytes(fis,fos, conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test 
	/**
	 * ������������, ʹ��FSDataOutputStreamЧ����ͬ IOUtils
	 * @Description 
	 * @author shkstart
	 * @date 2020��9��21������5:12:06
	 */
	public void testUpload(){ 
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			
			
			//to do operation
			FileInputStream fis = new FileInputStream("testRead_data2.txt");
			FSDataOutputStream fos = fs.create(new Path("/uploadtest/04.txt"));
			byte[] buffer = new byte[1024];
			int len;
			while((len = fis.read(buffer))!=-1){
				fos.write(buffer,0,len);
			}
			fis.close();
			fos.close(); 
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testdelete(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			//to do operation
			fs.delete(new Path("/uploadtest/01.txt"));
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test 
	public void testLs(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			//to do operation
			FileStatus[] listStatus = fs.listStatus(new Path("/uploadtest")); 
			for(FileStatus status : listStatus){
				System.out.println(status);
			}
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test 
	public void testLsr(){
		Configuration conf = new Configuration();
		try {
			HadoopUtil.Connection();
			FileSystem fs = FileSystem.get(HadoopUtil.getUrl(),conf);
			//to do operation
			 RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true); 
			while(listFiles.hasNext()){
				System.out.println(listFiles.next());
			}
			
			fs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}


