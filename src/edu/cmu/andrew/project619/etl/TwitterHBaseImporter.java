package edu.cmu.andrew.project619.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import edu.cmu.andrew.project619.db.HBaseTwitterTable;
import edu.cmu.andrew.project619.model.Twitter;
import edu.cmu.andrew.project619.util.RowKeyConverter;
import edu.cmu.andrew.project619.util.TimeFormater;

public class TwitterHBaseImporter extends Configured implements Tool {

	static class HBaseTwitterMapper<K, V> extends MapReduceBase implements
			Mapper<LongWritable, Text, K, V> {

		private HTable table;

		public void map(LongWritable key, Text value,
				OutputCollector<K, V> output, Reporter reporter)
				throws IOException {
			Twitter twitter = new Twitter(value.toString());
			if (twitter.isValid()) {
				System.out.println(twitter);
				byte[] rowKey = RowKeyConverter
						.makeTwitterRowKey(twitter.getTime(),
								twitter.getUserId(), twitter.getId());

				Put p = new Put(rowKey);

				p.add(HBaseTwitterTable.TWITTER_COLUMNFAMILY,
						HBaseTwitterTable.UID_QUALIFIER,
						Bytes.toBytes(twitter.getUserId() + ""));
				p.add(HBaseTwitterTable.TWITTER_COLUMNFAMILY,
						HBaseTwitterTable.TIME_QUALIFIER, Bytes
								.toBytes(TimeFormater.formatTime(twitter
										.getTime())));
				p.add(HBaseTwitterTable.TWITTER_COLUMNFAMILY,
						HBaseTwitterTable.TID_QUALIFIER,
						Bytes.toBytes(twitter.getId() + ""));

				table.put(p);

			}

		}

		public void configure(JobConf jc) {
			super.configure(jc);
			try {
				this.table = new HTable(HBaseConfiguration.create(jc), "twitter");
			} catch (IOException e) {
				throw new RuntimeException("Failed HTable construction", e);
			}
		}

		@Override
		public void close() throws IOException {

			table.close();
			super.close();
		}
	}

	public int run(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: TwitterHBaseImporter <input>");
			return -1;
		}
		JobConf jc = new JobConf(getConf(), getClass());
		FileInputFormat.addInputPath(jc, new Path(args[0]));

		jc.setMapperClass(HBaseTwitterMapper.class);
		jc.setNumReduceTasks(0);
		jc.setOutputFormat(NullOutputFormat.class);
		JobClient.runJob(jc);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(),
				new TwitterHBaseImporter(), args);
		System.exit(exitCode);
	}
}
