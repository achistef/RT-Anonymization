//package algorithms;
//
//import java.io.Serializable;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.broadcast.Broadcast;
//import scala.Tuple2;
//import struct.Details;
//import struct.Group;
//import struct.Instance;
//import struct.MySparkContext;
//import struct.Parser;
//import struct.ULmethod;
//
///**
// *
// * @author achilles
// */
//public class MergeTester implements Serializable
//{
//
//	public static void main(String[] args) throws Exception
//	{
//		JavaSparkContext sc = MySparkContext.getSparkContext();
//		sc.setCheckpointDir("checkpoint");
//
//		String datasetName = "s3://thesisspark/bigDataset";
//		String hPath = "s3://thesisspark/hierarchies/";
//
//		Details localMeta = new Details(hPath, datasetName);
//		localMeta.setK(10);
//		localMeta.setM(3);
//		localMeta.setRd(1);
//		localMeta.setMaxClusters(300);
//		localMeta.setUlFunction(ULmethod.hierarchy);
//
//		JavaRDD<String> input = sc.textFile(datasetName, localMeta.getNumOfPartitions());
//		String first = input.first();
//		input = input.filter((String s) -> !s.equals(first));
//		localMeta.setNumOfInstances((int) input.count());
//
//		final Broadcast<Details> meta = sc.broadcast(localMeta);
//		Parser parser = new Parser(meta);
//		JavaRDD<Instance> instances = input.map(parser);
//		JavaPairRDD<Long, Instance> dataset = instances.zipWithUniqueId().mapToPair((Tuple2<Instance, Long> t)
//			-> 
//			{
//				return new Tuple2<>(t._2, t._1);
//		});
//		System.out.println("Achilles Executing cluster...");
//		long clusterST = System.currentTimeMillis();
//		Cluster cluster = new Cluster(dataset, meta);
//		JavaRDD<Group> groups = cluster.createClusters().cache();
//		groups.count();
//		System.out.println("Achilles Cluster took " + (System.currentTimeMillis() - clusterST) + " ms to finish");
//
//		int i = 100;
//		MergeTesting mergeTesting = new MergeTesting(meta);
//		long t = System.currentTimeMillis();
//		mergeTesting.calc(groups, i);
//		long s = System.currentTimeMillis() - t;
//		double rate = s / (double) i;
//		double gain;
//		do
//		{
//			i += 100;
//			t = System.currentTimeMillis();
//			mergeTesting.calc(groups, i);
//			s = System.currentTimeMillis() - t;
//			double rate2 = s / (double) i;
//			gain = rate - rate2;
//			rate = rate2;
//		} while (gain > 0.3);
//		System.out.println("Achilles " + i);
//	}
//
//}
