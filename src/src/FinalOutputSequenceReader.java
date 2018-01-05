import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;

////File to be used for the folder clusteredPoints ---> contains the sequenceFile which has cluster id and data used for clustering in (key,value) format
public class FinalOutputSequenceReader {
	public static void main(String[] args) throws IOException {
	    String uri = "/home/training/mahout/testing/output/clusteredPoints/part-m-00000";
		//String uri ="/home/training/mahout/testing/inputFileSequence.txt";
	    //String uri="/home/training/mahout/testing/output/clusters-2-final/part-r-00000";
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    Path path = new Path(uri);

	    SequenceFile.Reader reader = null;
	    try {
	        reader = new SequenceFile.Reader(fs, path, conf);
	        System.out.println(reader.getValueClass());
	        Writable key = (Writable) ReflectionUtils.newInstance(
	                    reader.getKeyClass(), conf);
	        //Writable value = (Writable) ReflectionUtils.newInstance(
	        //            reader.getValueClass(), conf);
	        WeightedVectorWritable value=new WeightedVectorWritable();
	        //ClusterWritable value=new ClusterWritable();
	        long position = reader.getPosition();
	        while (reader.next(key, value)) {
	                System.out.println("Key: " + key + " value:" + ((WeightedVectorWritable)(value)).toString());
	                position = reader.getPosition();
	            }
	        
	        ///
	       /* for (    ClusterWritable value1 : new SequenceFileDirValueIterable<ClusterWritable>(s.getPath(),PathType.LIST,PathFilters.logsCRCFilter(),conf)) {
	            Cluster cluster=value.getValue();
	            clusters.add(cluster);
	          }
	          */
	        ///
	        } finally {
	            reader.close();
	    }
	}
}
