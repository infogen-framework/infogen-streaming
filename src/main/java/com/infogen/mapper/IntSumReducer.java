package com.infogen.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author larry/larrylv@outlook.com/创建时间 2015年12月9日 下午2:56:08
* @since 1.0
* @version 1.0
*/
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final Log LOGGER = LogFactory.getLog(WordCount.class);
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		LOGGER.info(key);
		context.write(key, result);
	}
}