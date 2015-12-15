package com.infogen.mapper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年12月9日 上午11:15:35
 * @since 1.0
 * @version 1.0
 */
public class WordCount {
	private static final Log LOGGER = LogFactory.getLog(WordCount.class);

	// Command line options
	private static Options opts = new Options();

	static {
		opts.addOption("skip", true, "忽略词");
		opts.addOption("lzo", false, "输入是否压缩");
		opts.addOption("in", true, "输入路径");
		opts.addOption("out", true, "输出路径");
		opts.addOption("help", false, "输出路径");
	}

	public static void main(String[] args) throws Exception {
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (cliParser.hasOption("help")) {
			new HelpFormatter().printHelp("WordCount", opts);
			System.exit(0);
		}
		if (!cliParser.hasOption("in") || !cliParser.hasOption("out")) {
			new HelpFormatter().printHelp("WordCount", opts);
			System.exit(0);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		String skip = cliParser.getOptionValue("skip");
		if (skip != null) {
			job.addCacheFile(new Path(skip).toUri());
			job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		}

		LOGGER.info(cliParser.getOptionValue("in"));
		LOGGER.info(cliParser.getOptionValue("out"));

		if (cliParser.hasOption("lzo")) {
			job.setInputFormatClass(LzoTextInputFormat.class);
			LzoTextInputFormat.addInputPath(job, new Path(cliParser.getOptionValue("in")));
		} else {
			FileInputFormat.addInputPath(job, new Path(cliParser.getOptionValue("in")));
		}
		Path out = new Path(cliParser.getOptionValue("out"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
