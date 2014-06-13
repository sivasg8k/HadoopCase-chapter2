package cookbook.hadoop.chapter2;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSWriter extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		String localInputPath = args[0];
		Path outputPath = new Path(args[1]);
		Configuration conf = getConf();
		/*conf.addResource(new Path("/home/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/home/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/home/hadoop/conf/mapred-site.xml"));*/
		FileSystem fs = FileSystem.get(conf);
		OutputStream os = fs.create(outputPath);
		InputStream is = new BufferedInputStream(new FileInputStream(
				localInputPath));
		IOUtils.copyBytes(is, os, conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new HDFSWriter(), args);
		System.exit(returnCode);
	}
}