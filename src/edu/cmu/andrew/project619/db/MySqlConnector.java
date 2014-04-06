package edu.cmu.andrew.project619.db;


/**
 * 
 * @author Tian Fangzheng
 */
public class MySqlConnector implements DBConnector {

    private Connection connect = null;

    public MySqlConnector() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connect = DriverManager.getConnection("jdbc:mysql://localhost/twitter","root","rainforest");
            
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }catch (SQLException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void closeConnection(){
        try {
            connect.close();
        } catch (SQLException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
  

    @Override
    public List<String> getTidByUidAndTime(String userid, String time) {
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
            return tweetID;
        } catch (SQLException ex) {
            Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE, null, ex);
        }
        return tweetID;
    }

    @Override
    public List<String> getRetweetUidByUid(String userId) {
	List<String> userID = new ArrayList<String>();
	try {
	    String query = "SELECT tid FROM twitts WHERE uid='" + userid;
	    Statement statement = (Statement) connect.createStatement();
	    ResultSet result = statement.executeQuery(query);
	    while (result.next()) {
		userID.add(result.getString("tid"));
	    }
	    Collections.sort(userID);
	    result.close();
	    statement.close();
	    return userID;
	} catch (SQLException ex) {
	    Logger.getLogger(MySqlConnector.class.getName()).log(Level.SEVERE,
		    null, ex);
	}
	return userID;
    }

	

}