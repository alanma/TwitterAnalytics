package edu.cmu.andrew.project619.db;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTwitterTable {
	
	public static final byte[] TWITTER_COLUMNFAMILY = Bytes.toBytes("t");
	public static final byte[] UID_QUALIFIER = Bytes.toBytes("uid");
	public static final byte[] TIME_QUALIFIER = Bytes.toBytes("time");
	public static final byte[] TID_QUALIFIER = Bytes.toBytes("tid");
	
}
