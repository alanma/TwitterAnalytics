package edu.cmu.andrew.project619.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.JSONObject;

import edu.cmu.andrew.project619.util.RowKeyConverter;
import edu.cmu.andrew.project619.util.TimeFormater;

public class Twitter {

	private boolean isValid;
	private Long id;
	private Long uid;
	private Long time;
	private Twitter retweet;
	private String location;
	private String text;

	public Twitter(String twitter) {
		try {
			JSONObject json = new JSONObject(twitter);
			this.id = json.getLong("id");
			this.uid = json.getJSONObject("user").getLong("id");
			this.time = TimeFormater
					.getTimeOffset(json.getString("created_at"));
			this.text = json.getString("text");
			//this.text=stripText(this.text);
			isValid = true;
			try{
				JSONObject place = json.getJSONObject("place");
				this.location = place.getString("name");
			}catch(Exception e){
				//e.printStackTrace();
			}
			
			if(json.has("retweeted_status")){
				this.setRetweet(new Twitter(json.getJSONObject("retweeted_status").toString()));
				//System.out.println(json.getJSONObject("retweeted_status").toString());
			}
			
		} catch (Exception e) {
			isValid = false;
			e.printStackTrace();
		}
	}

	private String stripText(String text) {
		StringBuilder sb=new StringBuilder();
		char[] chars=text.toCharArray();
		for(char c:chars){
			
		}
		return null;
	}

	public static void main(String[] args){
		try {
			FileReader fr=new FileReader(new File("/Users/miracle_tian/Google/Workspace/Twitter_HBase/part-00000"));
			BufferedReader br=new BufferedReader(fr);
			String ts;
			while((ts=br.readLine())!=null){
				Twitter twitter=new Twitter(ts);
				System.out.println(twitter.toString());
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
		
		
		
		
//		try {
//			FileReader fr=new FileReader(new File("/Users/miracle_tian/Google/Workspace/Twitter_HBase/part-00000"));
//			BufferedReader br=new BufferedReader(fr);
//			String ts;
//			while((ts=br.readLine())!=null){
//				Twitter twitter=new Twitter(ts);
//				//if (twitter.isValid()&&twitter.getRetweet()!=null) {
//					byte[] rowKey = RowKeyConverter
//							.makeTwitterRowKey(twitter.getTime(),twitter.getUserId(),twitter.getId());
//					for(byte b:rowKey){
//						System.out.print(String.format("%02X ", b));
//					}
//					System.out.println();
//				//}
//				
//				
//			}
//			//System.out.println(br.readLine());
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	public String toString() {
//		System.out.println(this.uid);
//		System.out.println(this.time);
//		//System.out.println(this.retweet.uid);
//		System.out.println(this.uid);

		return this.uid+"\t"+this.time+"\t"+this.id+"\t"+(this.retweet==null?"":this.retweet.uid)+"\t"+(this.location==null?"":this.location)+"\t"+this.text;
//		return TimeFormater.formatTime(this.time) + "\t" + this.uid + "\t"
//				+ this.id;
	}

	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getUid() {
		return uid;
	}

	public void setUid(Long uid) {
		this.uid = uid;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public Twitter getRetweet() {
		return retweet;
	}

	public void setRetweet(Twitter retweet) {
		this.retweet = retweet;
	}

}
