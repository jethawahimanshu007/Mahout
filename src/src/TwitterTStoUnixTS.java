import java.security.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class TwitterTStoUnixTS {
	public static long twitterStampToTimestamp(String twitterStamp){ 
		long timestamp = 0; 
		try { DateFormat formatter ;
		Date date;
		formatter = new SimpleDateFormat("EEE MMM d kk:mm:ss Z yyyy");
		date = (Date)formatter.parse(twitterStamp); 
		java.sql.Timestamp timeStampDate = new java.sql.Timestamp(date.getTime()); 
		timestamp = date.getTime() / 1000; } 
		catch (Exception e){e.printStackTrace();}

		return timestamp;
	  }
		
	public static void main(String args[])
	{
		System.out.println(twitterStampToTimestamp("Thu Jan 01 00:00:00 +0000 1970"));
	}
	}



