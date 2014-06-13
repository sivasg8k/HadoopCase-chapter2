package cookbook.hadoop.chapter2;
import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


//public class WeblogMapper extends MapReduceBase implements Mapper<LongWritable,Text,AvroKey<Long>,AvroValue<Utf8>> {
  public class WeblogMapper extends MapReduceBase implements Mapper<LongWritable,Text,AvroKey<Long>,AvroValue<WeblogEntry>> {
	
	//public void map(LongWritable key, Text value,OutputCollector<AvroKey<Long>,AvroValue<Utf8>> out,Reporter reporter) throws IOException {
			//out.collect(new AvroKey<Long>(key.get()),new AvroValue<Utf8>(new Utf8(value.toString())));
	  public void map(LongWritable key, Text value,OutputCollector<AvroKey<Long>,AvroValue<WeblogEntry>> out,Reporter reporter) throws IOException {
		  
		  String[] valSpl = value.toString().split(",");
		  
		  WeblogEntry wlEntry = new WeblogEntry();
		  wlEntry.setCookie(valSpl[0]);
		  wlEntry.setPage(valSpl[1]);
		  wlEntry.setDate(valSpl[2]);
		  wlEntry.setTime(valSpl[3]);
		  wlEntry.setIp(valSpl[4]);
		  
			
		  out.collect(new AvroKey<Long>(key.get()),new AvroValue<WeblogEntry>(wlEntry));
	}
}