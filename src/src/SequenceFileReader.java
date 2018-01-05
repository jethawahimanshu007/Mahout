import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;

//// Use this file to read the input file converted to it's equivalent sequence file of VectorWritables
public class SequenceFileReader {
	public static void main(String[] args) throws IOException {
	    //String uri = "/home/training/mahout/testing/output/clusters-1-final/part-r-00000";
		//String uri ="/home/training/mahout/testing/inputFileSequence.txt";
		String uri="/home/training/mahout/testing/twitterJsonForTimeSequence.txt";
		//String uri="/home/training/mahout/testing/output/clusters-0/part-00000";
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    Path path = new Path(uri);

	    SequenceFile.Reader reader = null;
	    try {
	        reader = new SequenceFile.Reader(fs, path, conf);
	        System.out.println(reader.getValueClass());
	        Writable key = (Writable) ReflectionUtils.newInstance(
	                    reader.getKeyClass(), conf);
	        Writable value = (Writable) ReflectionUtils.newInstance(
	                    reader.getValueClass(), conf);
	        long position = reader.getPosition();
	        while (reader.next(key, value)) {
	                System.out.println("Key: " + key + " value:" + value);
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
