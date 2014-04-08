package edu.cmu.andrew.project619.db;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRetweetTable {

	public static final byte[] RETWEET_COLUMNFAMILY = Bytes.toBytes("rt");
	//Original tweet's user id
	public static final byte[] OUID_QUALIFIER = Bytes.toBytes("ouid");
	//Retweet's user id
	public static final byte[] RUID_QUALIFIER = Bytes.toBytes("ruid");
	//Original tweet's id
	public static final byte[] OTID_QUALIFIER = Bytes.toBytes("otid");
	//Retweet's id
	public static final byte[] RTID_QUALIFIER = Bytes.toBytes("rid");

}
