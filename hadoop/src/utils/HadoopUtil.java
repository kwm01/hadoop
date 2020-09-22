package utils;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

public class HadoopUtil {
	private static String url,user,password; 
	public static void Connection() throws IOException{
		InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("hadoop.properties");
		Properties pros = new Properties();
		pros.load(is);
		url =pros.getProperty("url");
		user =pros.getProperty("user");
		password =pros.getProperty("password");
	}
	public static URI getUrl() throws Exception {
		return new URI(url);
	}
	public static String getUser() {
		return user;
	}
	public static String getPassword() {
		return password;
	}
}
