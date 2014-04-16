package edu.cmu.andrew.project619.util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;


public class RowKeyConverter {

	public static byte[] makeTwitterRowKey(Long uid, Long time, Long tid) {
		byte[] rowKey = new byte[24];
		byte[] uidBytes = new byte[8];
		Bytes.putLong(uidBytes, 0, uid);
		ArrayUtils.reverse(uidBytes);
		Bytes.putBytes(rowKey, 0, uidBytes, 0, 8);
		Bytes.putLong(rowKey, 8, time);
		Bytes.putLong(rowKey, 16, tid);
		return rowKey;
	}


	public static byte[] makeTwitterRowKey(Long userId, Long id) {
		byte[] rowKey = new byte[16];
		byte[] uidBytes = new byte[8];
		Bytes.putLong(uidBytes, 0, userId);
		ArrayUtils.reverse(uidBytes);
		Bytes.putBytes(rowKey, 0, uidBytes, 0, 8);
		Bytes.putLong(rowKey, 8, id);
		return rowKey;
	}
}
