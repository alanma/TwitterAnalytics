package edu.cmu.andrew.project619.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.json.JSONObject;

public class TwitterMySqlImporter {

	private String inputfilename = "part-00000";
	private String outputfilename = "output.csv";

	public void toCSV() {
		try {
			BufferedReader input = new BufferedReader(new FileReader(new File(
					inputfilename)));
			FileWriter output = new FileWriter(new File(outputfilename));
			output.write("uid, time, tid\n");
			String inputLine = null;
			while ((inputLine = input.readLine()) != null) {
				JSONObject twitter = new JSONObject(inputLine);
				String uid = twitter.getJSONObject("user").getString("id_str");
				String time = twitter.getString("created_at");
				String tid = twitter.getString("id_str");
				output.write(uid + ", " + time + ", " + tid + "\n");
			}
			input.close();
			output.close();
		} catch (Exception ex) {
		}
	}

	public void main(String[] args) {
		TwitterMySqlImporter importer = new TwitterMySqlImporter();
		importer.toCSV();
	}

}
