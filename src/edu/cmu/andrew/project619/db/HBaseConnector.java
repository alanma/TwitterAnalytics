package edu.cmu.andrew.project619.db;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author miracle_tian
 */

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import edu.cmu.andrew.project619.util.RowKeyConverter;
import edu.cmu.andrew.project619.util.TimeFormater;

public class HBaseConnector implements DBConnector{
	
    static HTable table=null;
    
    public HBaseConnector(String tableName) {
        Configuration config = HBaseConfiguration.create();  
		config.clear();
		config.set("hbase.zookeeper.quorum", "10.182.135.114");
        config.set("hbase.zookeeper.property.clientPort","2181");
        table = initHTable(config,tableName);
    }
    
    public HTable initHTable(Configuration config,String tableName) {
    	HTable newTable=null;
		try {
			 newTable= new HTable(config,tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return newTable;
	}
    
	@Override
	public void closeConnection() {
		try {
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	table=null;	
	}

	public List<String> getRetweetUidByUid(String uidStr){
		List<String> uids=new ArrayList<String>();
		long uid=Long.parseLong(uidStr);
		byte[] rowKeyStart = RowKeyConverter.makeTwitterRowKey(uid,0l, 0l);
        byte[] rowKeyStop = RowKeyConverter.makeTwitterRowKey(uid, Long.MIN_VALUE, Long.MIN_VALUE);
        
        Scan scan = new Scan();
        scan.setStartRow(rowKeyStart);
        scan.setStopRow(rowKeyStop);  
        ResultScanner scanner=null;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
   
        String ruid;
        HashSet<String> set=new HashSet<String>();
        try {
            for (Result result : scanner) {
                ruid = new String(result.getValue(HBaseRetweetTable.RETWEET_COLUMNFAMILY, HBaseRetweetTable.RUID_QUALIFIER));
                if(!set.contains(ruid)){
                	uids.add(ruid);
                	set.add(ruid);
                }
            }
        } finally {
            scanner.close();
        }
		return uids;
	}
	
	public List<String> getTidByUidAndTime(String userId, String time) {	
    	List<String> tids = new ArrayList<String>();
        Long timeOffset=0l;
		try {
			timeOffset = TimeFormater.getTimeOffsetFromQuery(time);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
        Long uid = Long.parseLong(userId);
        byte[] rowKeyStart = RowKeyConverter.makeTwitterRowKey(uid, timeOffset, 0l);
        byte[] rowKeyStop = RowKeyConverter.makeTwitterRowKey(uid, timeOffset, Long.MIN_VALUE);
        Scan scan = new Scan();
        scan.setStartRow(rowKeyStart);
        scan.setStopRow(rowKeyStop);  
        ResultScanner scanner=null;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
   
        String tid;
        try {
            for (Result result : scanner) {
                tid = new String(result.getValue(HBaseTwitterTable.TWITTER_COLUMNFAMILY, HBaseTwitterTable.TID_QUALIFIER));
                tids.add(tid);
            }
        } finally {
            scanner.close();
        }
        
        return tids;
    }

    public static void main(String[] args) throws IOException, ParseException {
        HBaseConnector c = new HBaseConnector("twitter");
        List<String> result = c.getTidByUidAndTime("2224894764", "2014-01-22 13:09:25");
        for (String s : result) {
            System.out.println(s);
        }
    }
    
    
}
