package edu.cmu.andrew.project619.model;

import org.json.JSONObject;

import edu.cmu.andrew.project619.util.TimeFormater;

public class Twitter {

	private boolean isValid;
	private Long id;
	private Long userId;
	private Long time;

	public Twitter(String twitter) {
		try {
			JSONObject json = new JSONObject(twitter);
			this.id = json.getLong("id");
			this.userId = json.getJSONObject("user").getLong("id");
			this.time = TimeFormater
					.getTimeOffset(json.getString("created_at"));
			isValid = true;
		} catch (Exception e) {
			isValid = false;
			e.printStackTrace();
		}
	}

	public String toString() {
		return TimeFormater.formatTime(this.time) + "\t" + this.userId + "\t"
				+ this.id;
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

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

}
