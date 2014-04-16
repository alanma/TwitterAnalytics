/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.cmu.andrew.project619.p1;

import edu.cmu.andrew.project619.db.DBConnector;
import edu.cmu.andrew.project619.db.MySqlConnector;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

/**
 *
 * @author fxqw8_000
 */
public class q2 extends HttpServlet {

    /**
	 * 
	 */
	private static final long serialVersionUID = 728956133897195165L;
	private static final String teamID = "Rainforest";
    private static final String AWSID = "2422-0942-6899";
    private MySqlConnector db=null;
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    
    @Override 
    public void init(){
    	InitialContext initCtx;
		try {
			initCtx = new InitialContext();
			DataSource dataSource = (DataSource) initCtx.lookup("java:comp/env/jdbc/TestDB");
			db=new MySqlConnector(dataSource);
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }
    
    @Override
    public void destroy(){
    	//db.closeConnection();
    	super.destroy();
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/plain;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            out.println(teamID  + ", " + AWSID);

            String userid = request.getParameter("userid");
            String time = request.getParameter("tweet_time");

            List<String> tweetID = db.getTidByUidAndTime(userid,time);
            
            for (String tweet : tweetID){
                out.println(tweet);
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
       
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }

}
