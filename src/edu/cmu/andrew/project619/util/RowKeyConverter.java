package edu.cmu.andrew.project619.util;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {

	public static byte[] makeTwitterRowKey(Long time, Long uid, Long tid) {

		byte[] rowKey = new byte[24];
		Bytes.putLong(rowKey, 0, time);
		Bytes.putLong(rowKey, 8, uid);
		Bytes.putLong(rowKey, 16, tid);
		return rowKey;
	}
}
