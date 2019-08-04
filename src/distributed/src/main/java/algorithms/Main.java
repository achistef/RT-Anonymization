package algorithms;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import metric.NcpCalculator;
import metric.UlCalculator;
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
import struct.ParserReverse;
import struct.SI_Map;
import struct.ULmethod;

/**
 *
 * @author achilles
 */
public class Main implements Serializable
{

	public static void main(String[] args) throws Exception
	{
		JavaSparkContext sc = MySparkContext.getSparkContext();
		sc.setCheckpointDir("checkpoint");

		String path = "s3://thesisspark/";
		String datasetName = path + "bigDataset";
		String hPath = path + "hierarchies/";

		Details localMeta = new Details(hPath, datasetName);
		localMeta.setK(10);
		localMeta.setM(3);
		//localMeta.setRd(0.7);
		//localMeta.setTd(0.5);
		localMeta.setUlFunction(ULmethod.hierarchy);
		localMeta.setMaxMerges(1000);
		localMeta.setMaxClusters(1000);

		//localMeta.setSplitter (",");
		//localMeta.setNumOfPartitions (12);
		//localMeta.setMaxConflicts (5);
		//localMeta.setMinMergeTreeSize (1);
		//localMeta.setMaxMergeTreeSize (5);
		JavaRDD<String> input = sc.textFile(datasetName, localMeta.getNumOfPartitions()).cache();
		String first = input.first();
		input = input.filter((String s) -> !s.equals(first));
		input = input.sample(false, 0.2, 123).cache();

		localMeta.setNumOfInstances((int) input.count());

		final Broadcast<Details> meta = sc.broadcast(localMeta);
		Parser parser = new Parser(meta);
		JavaRDD<Instance> instances = input.map(parser);
		JavaPairRDD<Long, Instance> dataset = instances.zipWithUniqueId().mapToPair((Tuple2<Instance, Long> t)
			-> 
			{
				return new Tuple2<>(t._2, t._1);
		});

		long start = System.currentTimeMillis();
		Cluster cluster = new Cluster(dataset, meta);
		JavaRDD<Group> groups = cluster.createClusters();
		Merge merge = new Merge(meta);
		groups = merge.calc(groups);
		TransactionalAnon tr = new TransactionalAnon(meta);
		groups = tr.anon(groups).cache();
		long end = System.currentTimeMillis() - start;
		System.out.println("total time " + end);

	}
//			Merge merge = new Merge(meta);
//			groups = merge.calc(groups);
//			TransactionalAnon tr = new TransactionalAnon(meta);
//			groups = tr.anon(groups).cache();
//			NcpCalculator ncpcalc = new NcpCalculator(meta, groups);
//			System.out.println("Achilles ncp : " + ncpcalc.ncpReadable());
//
//			UlCalculator ulcalc = new UlCalculator(meta, groups);
//			System.out.println("Achilles utility loss : " + ulcalc.ulReadable());
//			System.out.println("Achilles number of groups : " + groups.count());
//			System.out.println("Achilles number of rows : " + groups.aggregate(0, (Integer v, Group g) -> v + g.size(), (Integer v, Integer g) -> v + g));
//			ParserReverse pr = new ParserReverse(meta);
//			JavaRDD<String> map = groups.map(pr);
//			map.saveAsTextFile("result");
}
