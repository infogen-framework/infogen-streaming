package com.infogen.etl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年11月30日 下午4:43:18
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_ETL_Tracking_Hdfs {

	public static void main(String[] args) throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("mapred.output.compress", "true");
		configuration.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
		configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzopCodec");
		configuration.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
		Path path = new Path("hdfs://spark101:8020/test/output/demo/qqq8");

		DataOutputStream cmpOut = getIndexedLzoOutputStream(configuration, path);
		for (int i = 0; i < 134217728; i++) {
			cmpOut.write((Math.random() + "ok").getBytes());
		}

		cmpOut.close();

	}

	public static DataOutputStream getIndexedLzoOutputStream(Configuration conf, Path path) throws IOException {
		LzopCodec codec = new LzopCodec();
		codec.setConf(conf);
		final Path file = path;
		final FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		FSDataOutputStream indexOut = null;
		// if (conf.getBoolean("elephantbird.lzo.output.index", false)) {
		// }
		Path indexPath = file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
		indexOut = fs.create(indexPath, false);
		final boolean isIndexed = indexOut != null;
		OutputStream out = (isIndexed ? codec.createIndexedOutputStream(fileOut, indexOut) : codec.createOutputStream(fileOut));
		return new DataOutputStream(out) {
			public void close() throws IOException {
				super.close();
				if (isIndexed) {
					Path tmpPath = file.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
					FileStatus stat = fs.getFileStatus(file);
					if (stat.getLen() <= stat.getBlockSize()) {
						fs.delete(tmpPath, false);
					} else {
						fs.rename(tmpPath, file.suffix(LzoIndex.LZO_INDEX_SUFFIX));
					}
				}
			}
		};
	}

}
