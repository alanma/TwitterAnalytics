package edu.cmu.andrew.project619.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;


/**
 * 
 * @author Tian Fangzheng
 */
public class MySqlConnector implements DBConnector {

	DataSource ds;
    public MySqlConnector() {
        
    }
    
    public MySqlConnector(DataSource dataSource) {
		ds=dataSource;
	}

	public Connection getConnection(){
    	try {
            
            Connection connect = ds.getConnection();
            return connect;
       
        }catch (SQLException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
		return null;
    	
    }

    public void closeConnection(){
        
    }
  

    @Override
    public List<String> getTidByUidAndTime(String userid, String time) {
    	Connection connect=getConnection();
        List<String> tweetID = new ArrayList<String>();
        try {
            String query = "SELECT tid FROM twitts WHERE uid='" + userid 
                    + "' AND time=(SELECT DATE_FORMAT('" + time + "', '%a %b %d %T +0000 %Y'));";
            Statement statement = (Statement) connect.createStatement();
            ResultSet result = statement.executeQuery(query);
            while(result.next()){
                tweetID.add(result.getString("tid"));
            }
            Collections.sort(tweetID);
            result.close();
            statement.close();
            connect.close();
            return tweetID;
        } catch (SQLException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return tweetID;
    }

    @Override
    public List<String> getRetweetUidByUid(String userId) {
    	Connection connect=getConnection();
    	List<String> uidStr=new ArrayList<String>();
	List<Long> uids = new ArrayList<Long>();
	
	try {
	    String query = "SELECT DISTINCT uid FROM twitts WHERE original_uid='" + userId+"';";
	    Statement statement = (Statement) connect.createStatement();
	    ResultSet result = statement.executeQuery(query);
	    while (result.next()) {
		uids.add(Long.parseLong(result.getString("uid")));
	    }
	    Collections.sort(uids);
	    result.close();
	    statement.close();
	    connect.close();
	    for(Long l:uids){
	    	uidStr.add(l+"");
	    }    
	    return uidStr;
	} catch (SQLException ex) {
	    Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE,
		    null, ex);
	}
	return uidStr;
    }

	

}