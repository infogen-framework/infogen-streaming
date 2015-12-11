package com.infogen.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月9日 下午2:55:09
 * @since 1.0
 * @version 1.0
 */
public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final Log LOGGER = LogFactory.getLog(WordCount.class);

	private static enum CountersEnum {
		INPUT_WORDS
	}

	private boolean caseSensitive;
	private Set<String> patternsToSkip = new HashSet<String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
		if (conf.getBoolean("wordcount.skip.patterns", false)) {
			URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
			for (URI patternsURI : patternsURIs) {
				Path patternsPath = new Path(patternsURI.getPath());
				try (FileReader fr = new FileReader(patternsPath.getName().toString()); //
						BufferedReader fis = new BufferedReader(fr);) {
					String pattern = null;
					while ((pattern = fis.readLine()) != null) {
						patternsToSkip.add(pattern);
					}
				} catch (IOException ioe) {
					LOGGER.error("Caught exception while parsing the cached file '", ioe);
				}
			}
		}
	}

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		LOGGER.info("key:" + key.get());
		String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {
			String nextToken = itr.nextToken();
			if (!patternsToSkip.contains(nextToken)) {
				word.set(nextToken);
				context.write(word, one);
				LOGGER.info(nextToken);
				System.out.println(nextToken);
				Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
				counter.increment(1);
			}
		}
	}
}