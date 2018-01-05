import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


public class twitterJsonToSeqWithNamedVec {
	public static void main(String args[])
	{
		Configuration conf = new Configuration();
		FileSystem fs = null;
	try {
		fs = FileSystem.get(conf);
            //BufferedReader reader = new BufferedReader(new FileReader("testdata2/points.csv"));
			BufferedReader reader = new BufferedReader(new FileReader("/home/training/mahout/testing/twitter.json"));
           // SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("testdata2/points.seq"), LongWritable.class, VectorWritable.class);
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("/home/training/mahout/testing/twitterJsonForTimeSequence.txt"), LongWritable.class, VectorWritable.class);
            String line;
            long counter=0;
            while ((line = reader.readLine()) != null) {
            	JSONObject obj = new JSONObject(line);
        		double id=obj.getDouble("id");
            	//String[] c = line.split(",");
               /// double idDouble=Double.parseDouble(id);
                double createdAtDouble=twitterStampToTimestamp(obj.getString("created_at"));
                double[] d = new double[1];
                d[0] = createdAtDouble;
                Vector vec = new RandomAccessSparseVector(d.length);
                vec.assign(d);
                vec=new NamedVector(vec,Double.toString(id));
                VectorWritable writable = new VectorWritable();
                writable.set(vec);
                
                writer.append(new LongWritable(counter++), writable);
               /*
                if(c.length>1){
                    double[] d = new double[c.length];
                    for (int i = 0; i < c.length; i++)
                            d[i] = Double.parseDouble(c[i]);
                    Vector vec = new RandomAccessSparseVector(c.length);
                    vec.assign(d);

                VectorWritable writable = new VectorWritable();
                writable.set(vec);
                writer.append(new LongWritable(counter++), writable);
            }
            */
        }
        writer.close();
    
	}
	catch(Exception e)
	{
		System.out.println("Exception written by you has occured:"+e);
	}
	}
	
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
}
