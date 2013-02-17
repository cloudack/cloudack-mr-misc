/**
 * 
 */
package com.cloudack.misc;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 * @author pudi
 * 
 */
public class SequenceFileWriter {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		String uri = "seqfile";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		BytesWritable value = new BytesWritable();

		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
					value.getClass());
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				URL url = new URL(
						"https://www.google.com/images/srpr/logo3w.png");

				// Read the image ...
				InputStream inputStream = url.openStream();
				ByteArrayOutputStream output = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];

				int n = 0;
				while (-1 != (n = inputStream.read(buffer))) {
					output.write(buffer, 0, n);
				}
				inputStream.close();

				// Here's the content of the image...
				byte[] data = output.toByteArray();

				BytesWritable outfile = new BytesWritable(data);
				value.set(outfile);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}

	}

}
