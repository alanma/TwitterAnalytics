package edu.cmu.andrew.project619.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.cmu.andrew.project619.db.HBaseRetweetTable;
import edu.cmu.andrew.project619.db.HBaseTwitterTable;
import edu.cmu.andrew.project619.model.Twitter;
import edu.cmu.andrew.project619.util.RowKeyConverter;
import edu.cmu.andrew.project619.util.TimeFormater;

public class RetweetHBaseBulkLoader {

	public static class RetweetBulkLoadMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Twitter twitter = new Twitter(value.toString());
			if (twitter.isValid()&&twitter.getRetweet()!=null) {
				byte[] rowKey = RowKeyConverter
						.makeTwitterRowKey(twitter.getRetweet().getUserId(),twitter.getUserId(),twitter.getId());

				Put p = new Put(rowKey);
				p.add(HBaseRetweetTable.RETWEET_COLUMNFAMILY,HBaseRetweetTable.OUID_QUALIFIER,Bytes.toBytes(twitter.getRetweet().getUserId()+""));
				p.add(HBaseRetweetTable.RETWEET_COLUMNFAMILY,HBaseRetweetTable.OTID_QUALIFIER,Bytes.toBytes(twitter.getRetweet().getId()+""));
				p.add(HBaseRetweetTable.RETWEET_COLUMNFAMILY,HBaseRetweetTable.RUID_QUALIFIER,Bytes.toBytes(twitter.getUserId()+""));
				p.add(HBaseRetweetTable.RETWEET_COLUMNFAMILY,HBaseRetweetTable.RTID_QUALIFIER,Bytes.toBytes(twitter.getId()+""));

				
				ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
				context.write(HKey, p);

			}
	
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String inputPath = args[0];
		String outputPath = args[1];
		HTable hTable = new HTable(conf, args[2]);
		Job job = new Job(conf, "HBase_Bulk_loader");
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setJarByClass(RetweetHBaseBulkLoader.class);
		job.setMapperClass(RetweetBulkLoadMapper.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
