package edu.cmu.andrew.project619.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeFormater {

	public static Long getTimeOffset(String timeString) throws ParseException {
		DateFormat originalDateFormat = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
		Date date = originalDateFormat.parse(timeString);
		return date.getTime();
	}

	public static Long getTimeOffsetFromQuery(String timeString)
			throws ParseException {
		DateFormat newDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		newDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date date = newDateFormat.parse(timeString);
		return date.getTime();
	}

	public static String formatTime(String timeString) throws ParseException {
		DateFormat originalDateFormat = new SimpleDateFormat(
				"EEE MMM dd HH:mm:ss ZZZZZ yyyy");
		DateFormat newDateFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
		newDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date date = originalDateFormat.parse(timeString);
		return newDateFormat.format(date);
	}

	public static String formatTime(Long timeOffset) {
		Date date = new Date(timeOffset);
		DateFormat newDateFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
		newDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		return newDateFormat.format(date);
	}
}
