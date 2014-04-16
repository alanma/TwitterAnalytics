package edu.cmu.andrew.project619.etl;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

public class TwitterInterImporter {
	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		private Text mykey = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			JSONObject twitter = null;
			try {
				twitter = new JSONObject(line);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			StringBuilder builder = new StringBuilder();

			try {
				String uid = twitter.getJSONObject("user").getString("id_str");
				builder.append(uid);
				builder.append("\t");
				String time = twitter.getString("created_at");
				builder.append(time);
				builder.append("\t");
				String tid = twitter.getString("id_str");
				builder.append(tid);
				builder.append("\t");
				String retweet_uid = null;
				if (line.contains("retweeted_status")) {
					JSONObject retweet = twitter
							.getJSONObject("retweeted_status");
					retweet_uid = retweet.getJSONObject("user").getString(
							"id_str");
				}
				builder.append(retweet_uid);
				builder.append("\t");
				JSONObject place = twitter.getJSONObject("place");
				String location = place.getString("name");
				builder.append(location);
				builder.append("\t");
				String text = twitter.getString("text");
				StringBuilder textbuilder = new StringBuilder();
				int i = 0;
				int len = text.length();
				while(i < len){
					char tmp = text.charAt(i);
					if(Character.isLetter(tmp)){
						textbuilder.append(tmp);
					}else{
						textbuilder.append(" ");
					}
					i++;
				}
				text = textbuilder.toString();
				StringTokenizer st = new StringTokenizer(text);
				textbuilder = new StringBuilder();
				while(st.hasMoreTokens()){
					textbuilder.append(st.nextToken());
					textbuilder.append(" ");
				}
				text = textbuilder.toString();
				builder.append(text);
				
			} catch (JSONException e) {
				e.printStackTrace();
			}
			mykey.set(builder.toString());
			context.write(mykey, NullWritable.get());
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterator<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "phase3etl");
		job.setJarByClass(TwitterInterImporter.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
