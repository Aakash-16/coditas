package org.coditas.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable offset , Text line,Context context) throws IOException, InterruptedException{
		String lineStr = line.toString();
		String tokens[] = lineStr.split(" ");
		for (int count = 0 ; count <tokens.length;count++ ) {
			line.set(tokens[count]);
			context.write(line, new IntWritable(1));
		}
	}





}
