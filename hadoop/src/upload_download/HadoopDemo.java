package upload_download;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HadoopDemo {
	FileSystem fs;
	Configuration conf;
	@Before
	public void begin() throws IOException{
		// 加载src目录下的配置文件
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.229.168:9000");
		fs = FileSystem.get(conf);
	}
	
	@After
	public void end() throws IOException{
		fs.close();
	}
	
	
	public void mkdir() throws IOException {
		Path path = new Path("/test");
		fs.mkdirs(path);
	}
	
	public void upload() throws IOException {
		Path path = new Path("/test/test.jpg");
		FSDataOutputStream fsos = fs.create(path);
		FileUtils.copyFile(new File("C://Users/MFS/Desktop/test.jpg"), fsos);
	}
	
	public void download() throws IOException {
		Path path = new Path("/test/test.jpg");
		FSDataInputStream fsis = fs.open(path);
		FileUtils.copyInputStreamToFile(fsis, new File("C://Users/MFS/Desktop/test.jpg"));
		
	}
	@Test
	public void list() throws FileNotFoundException, IOException{
		Path path = new Path("/test");
		FileStatus[] sfs = fs.listStatus(path);
		for(FileStatus s :sfs){
			System.out.println(s.getPath() +"-----"+s.getLen()+"-----"+s.getAccessTime());
		}
	}
	
}
