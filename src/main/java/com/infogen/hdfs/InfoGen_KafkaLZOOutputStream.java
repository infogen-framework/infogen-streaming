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
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;

/**
 * 针对kafka实现使用lzo压缩的OutputStream
 * 
 * @author larry/larrylv@outlook.com/创建时间 2015年11月30日 下午4:43:18
 * @since 1.0
 * @version 1.0
 */
public class InfoGen_KafkaLZOOutputStream implements Delayed, InfoGen_OutputStream {
	private static Logger LOGGER = Logger.getLogger(InfoGen_KafkaLZOOutputStream.class);
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
	private Path path_index;
	private FileSystem fs;
	private String suffix = ".finish";
	private DataOutputStream lzoOutputStream;

	public static void main(String[] args) throws IllegalArgumentException, IOException {
		InfoGen_KafkaLZOOutputStream infogen_lzooutputstream = new InfoGen_KafkaLZOOutputStream(new Path("hdfs://spark101:8020/infogen/output/test"));
		for (int i = 0; i < 100000000; i++) {
			infogen_lzooutputstream.write_line("dfs://spark101:8020/infogen/output/test");
		}
		infogen_lzooutputstream.close();
	}

	public InfoGen_KafkaLZOOutputStream(Path path) throws IOException {
		this.path = path;
		this.path_index = path.suffix(LzoIndex.LZO_INDEX_SUFFIX);
		this.fs = path.getFileSystem(configuration);

		fs.delete(path, true);
		fs.delete(path.suffix(suffix), true);
		fs.delete(path.suffix(LzoIndex.LZO_INDEX_SUFFIX), true);

		LOGGER.info("#创建流-写入LZO文件并使用索引:" + path.toString());
		FSDataOutputStream fileOut = fs.create(path, false);
		FSDataOutputStream indexOut = fs.create(path_index, false);
		LzopCodec codec = new LzopCodec();
		codec.setConf(configuration);
		CompressionOutputStream createIndexedOutputStream = codec.createIndexedOutputStream(fileOut, indexOut);
		lzoOutputStream = new DataOutputStream(createIndexedOutputStream);
	}

	private final byte[] lock = new byte[0];

	public Boolean write_line(String message) throws IOException {
		synchronized (lock) {
			if (lzoOutputStream != null) {
				lzoOutputStream.write(InfoGen_KafkaLZOOutputStream.newline);
				lzoOutputStream.write(message.getBytes(InfoGen_KafkaLZOOutputStream.utf8));
				return true;
			} else {
				return false;
			}
		}
	}

	public void close() throws IOException {
		synchronized (lock) {
			if (lzoOutputStream != null) {
				LOGGER.info("#关闭流-写入LZO文件并使用索引:" + path.toString());
				lzoOutputStream.close();
				lzoOutputStream = null;

				Path new_path = path.suffix(suffix);
				fs.rename(path, new_path);

				FileStatus stat = fs.getFileStatus(new_path);
				if (stat.getLen() <= (stat.getBlockSize() * 1.2)) {
					fs.delete(path_index, false);
				} else {
					fs.rename(path_index, new_path.suffix(LzoIndex.LZO_INDEX_SUFFIX));
				}
			} else {
				LOGGER.error("#流已经关闭-写入LZO文件并使用索引:" + path.toString());
			}
		}
	}

	public Long delay_millis = Clock.systemUTC().millis() + 60 * 1000;// 到期时间

	@Override
	public int compareTo(Delayed o) {
		if (this.delay_millis < ((InfoGen_KafkaLZOOutputStream) o).delay_millis) {
			return -1;
		} else if (this.delay_millis > ((InfoGen_KafkaLZOOutputStream) o).delay_millis) {
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
