package cookbook.hadoop.chapter2;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


public class AvroReader {

	public static void main(String[] args) throws IOException {
		
		Schema.Parser parser = new Schema.Parser();
		Schema schema2 = parser.parse(new File("/home/sivashankar/software/avro/WeblogEntry.avsc"));
		
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema2);
		DataFileReader<GenericRecord> dataReader = new DataFileReader<GenericRecord>(new File("/home/sivashankar/workspace/HadoopCases/src/main/resources/output/part-00000.avro"), reader);
		while(dataReader.hasNext())
		{
			GenericRecord record = dataReader.next();
			System.out.println("Reading: \n"+record);
		}

	}

}
