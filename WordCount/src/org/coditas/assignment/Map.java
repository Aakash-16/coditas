package org.coditas.assignment;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable offset , Text line,Context context){
		
	}
		
	
	
	

}
