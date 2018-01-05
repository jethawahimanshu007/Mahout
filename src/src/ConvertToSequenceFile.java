import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

////This file is used to convert an input file to it's equivalent sequence file of VectorWritables 

public class ConvertToSequenceFile {
	public static void main(String args[])
	{
		Configuration conf = new Configuration();
		FileSystem fs = null;
	try {
		fs = FileSystem.get(conf);
            //BufferedReader reader = new BufferedReader(new FileReader("testdata2/points.csv"));
			BufferedReader reader = new BufferedReader(new FileReader("/home/training/mahout/testing/inputFile.txt"));
           // SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("testdata2/points.seq"), LongWritable.class, VectorWritable.class);
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("/home/training/mahout/testing/inputFileSequence.txt"), LongWritable.class, VectorWritable.class);
            String line;
            long counter=0;
            while ((line = reader.readLine()) != null) {
                String[] c = line.split(",");
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
        }
        writer.close();
    
	}
	catch(Exception e)
	{
		System.out.println("Exception written by you has occured:"+e);
	}
	}
}
