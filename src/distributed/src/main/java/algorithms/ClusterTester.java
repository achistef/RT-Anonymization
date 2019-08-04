package algorithms;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import struct.Details;
import struct.Group;
import struct.Instance;
import struct.MySparkContext;
import struct.Parser;
import struct.ULmethod;

/**
 *
 * @author achilles
 */
public class ClusterTester
{

	public static void main(String[] args) throws Exception
	{

		JavaSparkContext sc = MySparkContext.getSparkContext();
		sc.setCheckpointDir("checkpoint");

		String datasetName = "s3://thesisspark/bigDataset";
		String hPath = "s3://thesisspark/hierarchies/";
//		String datasetName = "/home/achilles/Downloads/spark-2.1.0-bin-hadoop2.7/bigDataset";
//		String hPath = "/home/achilles/Downloads/spark-2.1.0-bin-hadoop2.7/hierarchies/";

		Details localMeta = new Details(hPath, datasetName);
		localMeta.setK(10);

		JavaRDD<String> input = sc.textFile(datasetName, localMeta.getNumOfPartitions());
		String first = input.first();
		input = input.filter((String s) -> !s.equals(first));
		localMeta.setNumOfInstances((int) input.count());

		final Broadcast<Details> meta = sc.broadcast(localMeta);
		Parser parser = new Parser(meta);
		JavaRDD<Instance> instances = input.map(parser);
		JavaPairRDD<Long, Instance> dataset = instances.zipWithUniqueId().mapToPair((Tuple2<Instance, Long> t)
			-> 
			{
				return new Tuple2<>(t._2, t._1);
		}).cache();
		
		int i = 100;
		long t = System.currentTimeMillis();
		ClusterTesting cluster = new ClusterTesting(dataset, meta);
		JavaRDD<Group> groups = cluster.createClusters(i);
		long s = System.currentTimeMillis() - t;
		double rate = s / (double) i;
		double gain;
		do
		{
			i += 100;
			t = System.currentTimeMillis();
			cluster = new ClusterTesting(dataset, meta);
			groups = cluster.createClusters(i);
			s = System.currentTimeMillis() - t;
			System.out.println("round time "+ s/1000.0);
			double rate2 = s / (double) i;
			gain = rate - rate2;
			rate = rate2;
		} while (gain > 5);
		System.out.println("max clusters = " + i);
	}

}
