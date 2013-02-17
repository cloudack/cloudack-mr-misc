/**
 * 
 */
package com.cloudack.misc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pudi
 * 
 */

public class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line, ":");
		String ckey = tokenizer.nextToken();
		String cVal = tokenizer.nextToken();

		StringTokenizer cTokenizer = new StringTokenizer(cVal, " ");
		while (cTokenizer.hasMoreTokens()) {
			String cValElement = cTokenizer.nextToken();
			if (cValElement.compareTo(ckey) > 0) {
				context.write(new Text(ckey + ":" + cValElement),
						new Text(cVal));
				
			} else {
				context.write(new Text(cValElement + ":" + ckey),
						new Text(cVal));
				
			}
		}

		//FileOutputFormat.getWorkOutputPath(context);
	}
}
