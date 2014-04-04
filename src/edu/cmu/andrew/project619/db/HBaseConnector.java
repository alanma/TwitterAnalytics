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
    
    public HBaseConnector() {
        Configuration config = HBaseConfiguration.create();  
		config.clear();
		config.set("hbase.zookeeper.quorum", "10.180.146.104");
        config.set("hbase.zookeeper.property.clientPort","2181");
        table = initHTable(config);
    }
    
    public HTable initHTable(Configuration config) {
    	HTable newTable=null;
		try {
			 newTable= new HTable(config,"twitter");
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

	public List<String> getTidByUidAndTime(String userId, String time) {	
    	List<String> tids = new ArrayList<String>();
        Long timeOffset=0l;
		try {
			timeOffset = TimeFormater.getTimeOffsetFromQuery(time);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
        Long uid = Long.parseLong(userId);
        byte[] rowKeyStart = RowKeyConverter.makeTwitterRowKey(timeOffset, uid, 0l);
        byte[] rowKeyStop = RowKeyConverter.makeTwitterRowKey(timeOffset, uid, Long.MIN_VALUE);
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
        HBaseConnector c = new HBaseConnector();
        List<String> result = c.getTidByUidAndTime("2224894764", "2014-01-22 13:09:25");
        for (String s : result) {
            System.out.println(s);
        }

    }
}
