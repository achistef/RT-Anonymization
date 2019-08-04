package algorithms;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import metric.ClusterCombiner;
import metric.NcpGroupCalculator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import metric.RatioGroupCalculator;
import metric.UlGroupCalculator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple7;
import struct.Details;
import struct.Group;
import struct.MySparkContext;
import struct.MyTreeMap;

/**
 *
 * @author achilles
 */
public class MergeTesting implements Serializable
{

	private final Broadcast<Details> meta;

	public MergeTesting(Broadcast<Details> meta)
	{
		this.meta = meta;
	}

	public void calc(JavaRDD<Group> input, int testCounter) throws IOException, InterruptedException
	{
		input.cache();
		NcpGroupCalculator ncpCalc = new NcpGroupCalculator(meta);
		UlGroupCalculator ulCalc = new UlGroupCalculator(meta);
		RatioGroupCalculator ratioCalc = new RatioGroupCalculator();

		JavaRDD<Tuple5<Double, BigDecimal, Group, Long, BigDecimal>> groups = input.map(ncpCalc).map(ulCalc).map(ratioCalc).zipWithUniqueId()
			.map((Tuple2<Tuple4<Double, BigDecimal, Group, BigDecimal>, Long> v)
				-> new Tuple5<>(v._1._1(), v._1._2(), v._1._3(), v._2, v._1._4())).cache();

		int minTreeSize = this.meta.value().getMinMergeTreeSize();
		int treeSize = minTreeSize;

		int merges = testCounter;

		MyTreeMap<BigDecimal, Tuple4<Double, BigDecimal, Group, Long>> aggregate = groups.aggregate(new MyTreeMap<>(merges),
			(MyTreeMap<BigDecimal, Tuple4<Double, BigDecimal, Group, Long>> v1, Tuple5<Double, BigDecimal, Group, Long, BigDecimal> v2)
			-> 
			{
				v1.put(v2._5(), new Tuple4<>(v2._1(), v2._2(), v2._3(), v2._4()));
				return v1;
		}, (MyTreeMap<BigDecimal, Tuple4<Double, BigDecimal, Group, Long>> v1, MyTreeMap<BigDecimal, Tuple4<Double, BigDecimal, Group, Long>> v2)
			-> 
			{
				v2.entrySet().forEach((entry)
					-> 
					{
						entry.getValue().forEach((tuple3)
							-> 
							{
								v1.put(entry.getKey(), tuple3);
						});
				});
				return v1;
		});

		ArrayList<Tuple4<Double, BigDecimal, Group, Long>> centers = new ArrayList<>();
		aggregate.values().forEach((entrySet)
			-> 
			{
				entrySet.forEach((value)
					-> 
					{
						centers.add(new Tuple4<>(value._1(), value._2(), value._3(), value._4()));
				});
		});

		Broadcast<ArrayList<Tuple4<Double, BigDecimal, Group, Long>>> brCenters = MySparkContext.getSparkContext().broadcast(centers);
		ClusterCombiner mergeFinder = new ClusterCombiner(meta, brCenters, treeSize);

		JavaPairRDD<Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>> reduceByKey = groups.mapPartitionsToPair(mergeFinder).reduceByKey(
			(Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group> v1, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group> v2)
			-> 
			{
				return v1._1().compareTo(v2._1()) < 0 ? v1 : v2;
		});

		List<Tuple2<Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>>> match = reduceByKey.collect();

		Map<Long, Tuple4<Double, BigDecimal, Group, BigDecimal>> mergedGroups = new HashMap<>();
		Set<Long> remove = new HashSet<>();

		for (Tuple2<Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>> tuple : match)
		{
			Long centerID = tuple._1();
			if (!remove.contains(tuple._2._4()))
			{
				for (Tuple4<Double, BigDecimal, Group, Long> temp : centers)
				{
					if (temp._4().equals(centerID))
					{
						Group mergedGroup = tuple._2._7();
						double ncpMerged = tuple._2._5();
						remove.add(tuple._2._4());
						BigDecimal ratio = tuple._2._6().compareTo(BigDecimal.ZERO) != 0 ? new BigDecimal(ncpMerged).divide(tuple._2._6(), 200, RoundingMode.CEILING) : new BigDecimal("10000");
						mergedGroups.put(centerID, new Tuple4<>(ncpMerged, tuple._2._6(), mergedGroup, ratio));
						break;

					}
				}
			}
		}

		Broadcast<Set<Long>> brUsed = MySparkContext.getSparkContext().broadcast(remove);
		Broadcast<Map<Long, Tuple4<Double, BigDecimal, Group, BigDecimal>>> brMergedGroups = MySparkContext.getSparkContext().broadcast(mergedGroups);

		groups.filter((Tuple5<Double, BigDecimal, Group, Long, BigDecimal> t1) -> !brUsed.value().contains(t1._4()))
			.map((Tuple5<Double, BigDecimal, Group, Long, BigDecimal> t1)
				-> 
				{
					Tuple4<Double, BigDecimal, Group, BigDecimal> get = brMergedGroups.value().get(t1._4());
					if (get == null)
					{
						return t1;
					} else
					{
						return new Tuple5<>(get._1(), get._2(), get._3(), t1._4(), get._4());
					}
			}).count();
	}
}
