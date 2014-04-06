package edu.cmu.andrew.project619.db;


public interface DBConnector {
	
	public List<String> getTidByUidAndTime(String userId, String time);
	public void closeConnection();

    public List<String> getRetweetUidByUid(String userId);
	
}