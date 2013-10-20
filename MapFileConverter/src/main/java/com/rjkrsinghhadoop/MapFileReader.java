package com.rjkrsinghhadoop;

/****************************************
 * MapFileLookup.java 
 * **************************************/

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

public class MapFileReader {

  
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		FileSystem fs = null;
		Text txtKey = new Text(args[1]);
		Text txtValue = new Text();
		MapFile.Reader reader = null;

		try {
			fs = FileSystem.get(conf);

			try {
				reader = new MapFile.Reader(fs, args[0].toString(), conf);
				reader.get(txtKey, txtValue);
			} catch (IOException e) {
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("The value for Key "+ txtKey.toString() +" is "+ txtValue.toString());
	}
}