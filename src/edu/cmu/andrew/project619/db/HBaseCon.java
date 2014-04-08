package edu.cmu.andrew.project619.db;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import edu.cmu.andrew.project619.util.RowKeyConverter;
import edu.cmu.andrew.project619.util.TimeFormater;



public class HBaseCon {
	
	Configuration config;
	
	
	public HBaseCon(){
		config=HBaseConfiguration.create();
		config.clear();
//		//config.set("user.name", "hadoop");
		config.set("hbase.zookeeper.quorum", "ec2-174-129-162-157.compute-1.amazonaws.com");
        config.set("hbase.zookeeper.property.clientPort","2181");
//        config.set("hbase.master", "ec2-107-22-156-239.compute-1.amazonaws.com:60000");
        
	}
	
	public List<String> getTidByUidAndTime(String userId, String time) throws IOException, ParseException{
		HTable table = new HTable(config, Bytes.toBytes("twitter"));
		Long timeOffset=TimeFormater.getTimeOffsetFromQuery(time);
		Long uid=Long.parseLong(userId);
		byte[] rowKeyStart=RowKeyConverter.makeTwitterRowKey(uid,timeOffset,0l);
		byte[] rowKeyStop=RowKeyConverter.makeTwitterRowKey(uid,timeOffset,Long.MIN_VALUE);
		Scan scan=new Scan();
		scan.setStartRow(rowKeyStart);
		scan.setStopRow(rowKeyStop);
		
		ResultScanner scanner = table.getScanner(scan);
		
		List<String> tids=new ArrayList<String>();
		String tid;
		try {
			for (Result result: scanner){ 
				tid=new String(result.getValue(HBaseTwitterTable.TWITTER_COLUMNFAMILY,HBaseTwitterTable.TID_QUALIFIER));
				//System.out.println(new String(result.getValue(HBaseTwitterTable.TWITTER_COLUMNFAMILY,HBaseTwitterTable.TID_QUALIFIER)));
				tids.add(tid);
			}
		} finally {
			scanner.close(); 
		}
		table.close();
		return tids;
	}
	
	
	public static void main(String[] args) throws IOException, ParseException {
		HBaseCon c=new HBaseCon();
		List<String> result=c.getTidByUidAndTime("2224894764", "2014-01-22+13:09:25");
		System.out.println(result.size());
		for(String s:result){
			System.out.println(s);
		}

	}
	
	

}
