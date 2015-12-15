package com.infogen.hdfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Clock;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * @author larry/larrylv@outlook.com/创建时间 2015年11月30日 下午4:43:18
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_LZOOutputStream implements Delayed, InfoGen_OutputStream {
	private static Logger LOGGER = Logger.getLogger(InfoGen_LZOOutputStream.class);
	public static final String utf8 = "UTF-8";
	public static final byte[] newline;
	public static final byte[] keyValueSeparator;

	static {
		try {
			newline = "\n".getBytes(utf8);
			keyValueSeparator = "\t".getBytes(utf8);
		} catch (UnsupportedEncodingException uee) {
			throw new IllegalArgumentException("can't find " + utf8 + " encoding");
		}
	}

	private Configuration configuration = new Configuration();
	private Path path;
	private Path path_index_tmp;
	private FileSystem fs;
	private DataOutputStream lzoOutputStream;

	public InfoGen_LZOOutputStream() {

	}

	public InfoGen_LZOOutputStream(Path path) throws IOException {
		configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,com.hadoop.compression.lzo.LzopCodec");
		configuration.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzopCodec");
		this.path = path;
		this.path_index_tmp = path.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX);
		this.fs = path.getFileSystem(configuration);

		fs.delete(path, true);
		fs.delete(path.suffix(LzoIndex.LZO_TMP_INDEX_SUFFIX), true);
		fs.delete(path.suffix(LzoIndex.LZO_INDEX_SUFFIX), true);

		LOGGER.info("#创建流-写入LZO文件并使用索引:" + path.toString());
		FSDataOutputStream fileOut = fs.create(path, false);
		FSDataOutputStream indexOut = fs.create(path_index_tmp, false);

		LzopCodec codec = new LzopCodec();
		codec.setConf(configuration);
		lzoOutputStream = new DataOutputStream(codec.createIndexedOutputStream(fileOut, indexOut));
	}

	private final byte[] lock = new byte[0];

	public Boolean write_line(String message) throws IOException {
		synchronized (lock) {
			if (lzoOutputStream != null) {
				lzoOutputStream.write(InfoGen_LZOOutputStream.newline);
				lzoOutputStream.write(message.getBytes(InfoGen_LZOOutputStream.utf8));
				return true;
			} else {
				return false;
			}
		}
	}

	public Integer size() {
		synchronized (lock) {
			if (lzoOutputStream != null) {
				return lzoOutputStream.size();
			}
		}
		return 0;
	}

	public Path getPath() {
		return path;
	}

	public void close(String suffix) throws IOException {
		synchronized (lock) {
			if (lzoOutputStream != null) {
				LOGGER.info("#关闭流-写入LZO文件并使用索引:" + path.toString());
				lzoOutputStream.close();
				lzoOutputStream = null;

				Path new_path = path.suffix(suffix);
				fs.rename(path, new_path);

				FileStatus stat = fs.getFileStatus(new_path);
				if (stat.getLen() <= stat.getBlockSize()) {
					fs.delete(path_index_tmp, false);
				} else {
					fs.rename(path_index_tmp, new_path.suffix(LzoIndex.LZO_INDEX_SUFFIX));
				}
			} else {
				LOGGER.error("#流已经关闭-写入LZO文件并使用索引:" + path.toString());
			}
		}
	}

	public Long delay_millis = Clock.systemUTC().millis() + 60 * 1000;// 到期时间

	@Override
	public int compareTo(Delayed o) {
		if (this.delay_millis < ((InfoGen_LZOOutputStream) o).delay_millis) {
			return -1;
		} else if (this.delay_millis > ((InfoGen_LZOOutputStream) o).delay_millis) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(delay_millis - Clock.systemUTC().millis(), TimeUnit.MILLISECONDS);
	}
}
