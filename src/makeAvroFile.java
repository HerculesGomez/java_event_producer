import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import headshot.avro.Headshot;

public class makeAvroFile {

	public static void main(String[] args) throws IOException {
		// Create our avro file
		File file = new File("./src/headshots.avro");
		
		// make some headshots
		Headshot hs1 = new Headshot();
		hs1.setUserId(100);
		hs1.setName("HerculesGomez");
		hs1.setGameId(1);
		hs1.setMapId(5);
		hs1.setTimestamp(new java.util.Date().getTime());
		

		// Alternate constructor
		Headshot hs2 = new Headshot(150, "BillyPanda", 2, 3, new java.util.Date().getTime());
		
		
		// Write out the headshots to the file
		DatumWriter<Headshot> headshotDatumWriter = new SpecificDatumWriter<Headshot>(Headshot.class);
		DataFileWriter<Headshot> dataFileWriter = new DataFileWriter<Headshot>(headshotDatumWriter);
		dataFileWriter.create(hs1.getSchema(), file);
		dataFileWriter.append(hs1);
		dataFileWriter.append(hs2);
		dataFileWriter.close();
		
		
		// Deserialize Headshots from disk
		DatumReader<Headshot> headshotDatumReader = new SpecificDatumReader<Headshot>(Headshot.class);
		DataFileReader<Headshot> dataFileReader = new DataFileReader<Headshot>(file, headshotDatumReader);
		Headshot headshot = null;
		while (dataFileReader.hasNext()) 
		{
		// Reuse user object by passing it to next(). This saves us from
		// allocating and garbage collecting many objects for files with
		// many items.
		headshot = dataFileReader.next(headshot);
		System.out.println(headshot);
		}

	}

}
