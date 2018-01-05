import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;


public class ConvertClusterFileToSequenceFile {
	public static void main(String args[])
	{
		Configuration conf = new Configuration();
		FileSystem fs = null;
	try {
		fs = FileSystem.get(conf);
            //BufferedReader reader = new BufferedReader(new FileReader("testdata2/points.csv"));
			BufferedReader reader = new BufferedReader(new FileReader("/home/training/mahout/testing/clusterIP.txt"));
           // SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("testdata2/points.seq"), LongWritable.class, VectorWritable.class);
            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,new Path("/home/training/mahout/testing/clusterIpSequence.txt"), ClusterWritable.class, Kluster.class);
            String line;
            long counter=0;
            int lineNumber=1;
            while ((line = reader.readLine()) != null) {
            	
                String[] c = line.split(",");
                if(c.length>1){
                    double[] d = new double[c.length];
                    for (int i = 0; i < c.length; i++)
                            d[i] = Double.parseDouble(c[i]);
                    Vector vec = new RandomAccessSparseVector(c.length);
                    vec.assign(d);
                    Kluster cluster = new Kluster(vec, lineNumber, new EuclideanDistanceMeasure());
                ClusterWritable writable = new ClusterWritable();
                writable.setValue(cluster);
                //writer.append(new LongWritable(counter++), writable);
                writer.append(writable,cluster );
                lineNumber++;
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
