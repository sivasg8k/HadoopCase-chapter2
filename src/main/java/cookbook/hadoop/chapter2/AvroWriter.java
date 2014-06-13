package cookbook.hadoop.chapter2;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroWriter extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		
		
		Schema outSchema = ReflectData.get().getSchema(WeblogEntry.class);
		//Schema SCHEMA = Pair.getPairSchema(Schema.create(Schema.Type.LONG),Schema.create(Schema.Type.STRING));
		Schema SCHEMA = Pair.getPairSchema(Schema.create(Schema.Type.LONG),outSchema);

		
		JobConf job = new JobConf();
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		outputPath.getFileSystem(job).delete(outputPath);
	    
	    // configure input for non-Avro sequence file
	    job.setInputFormat(TextInputFormat.class);
	    FileInputFormat.setInputPaths(job, inputPath);

	    // use a hadoop mapper that emits Avro output
	    job.setMapperClass(WeblogMapper.class);

	    // reducer is default, identity

	    // configure output for avro
	    FileOutputFormat.setOutputPath(job, outputPath);
	    AvroJob.setOutputSchema(job, SCHEMA);

	    JobClient.runJob(job);
		
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new AvroWriter(), args);
		System.exit(returnCode);
	}
}