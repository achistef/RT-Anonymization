package algorithms;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import metric.NcpCalculator;
import metric.ClusterCombiner;
import metric.NcpGroupCalculator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import metric.RatioGroupCalculator;
import metric.UlCalculator;
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
public class Merge implements Serializable
{

	private final Broadcast<Details> meta;

	public Merge(Broadcast<Details> meta)
	{
		this.meta = meta;
	}

	public JavaRDD<Group> calc(JavaRDD<Group> input) throws IOException, InterruptedException
	{
		input.cache();
		NcpCalculator totalNcpCalc = new NcpCalculator(meta, input);
		UlCalculator totalUlCalc = new UlCalculator(meta, input);
		double totalNcp = totalNcpCalc.ncp();
		BigDecimal totalUl = totalUlCalc.ul();
		long numOfGroups = input.count();
		NcpGroupCalculator ncpCalc = new NcpGroupCalculator(meta);
		UlGroupCalculator ulCalc = new UlGroupCalculator(meta);
		RatioGroupCalculator ratioCalc = new RatioGroupCalculator();

		JavaRDD<Tuple5<Double, BigDecimal, Group, Long, BigDecimal>> groups = input.map(ncpCalc).map(ulCalc).map(ratioCalc).zipWithUniqueId()
			.map((Tuple2<Tuple4<Double, BigDecimal, Group, BigDecimal>, Long> v)
				-> new Tuple5<>(v._1._1(), v._1._2(), v._1._3(), v._2, v._1._4())).cache();

		boolean changes;
		int minTreeSize = this.meta.value().getMinMergeTreeSize();
		int maxTreeSize = this.meta.value().getMaxMergeTreeSize();
		int treeSize = minTreeSize;
		int merges = this.meta.value().getMaxMerges();
		int iterationCounter = 0;
		long start = System.currentTimeMillis();
		BigDecimal v = BigDecimal.ZERO;
		do
		{
			BigDecimal testerul = totalUl.divide(this.meta.value().getTransactionSR().multiply(new BigDecimal(this.meta.value().getNumOfInstances())), 5, RoundingMode.CEILING).setScale(5, BigDecimal.ROUND_DOWN);
			double testerncp = totalNcp / (this.meta.value().getNumOfInstances() * this.meta.value().getNumOfAttributes());
			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(5);
			df.setMinimumFractionDigits(5);
			String format = df.format(testerul);
			double time = (System.currentTimeMillis() - start) / 1000;
			System.out.println("Achilles " + String.format(format) + "," + String.format("%.2f", testerncp));

			if (numOfGroups < merges + 1)
			{
				if (numOfGroups == 1)
				{
					break;
				} else
				{
					merges = (int) (numOfGroups / 2);
				}
			}

			changes = false;
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

			BigDecimal gain = BigDecimal.ZERO;
			int mergeCounter = 0;
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
							double difference = tuple._2._2();
							if (totalNcp + difference < this.meta.value().getRd())
							{
								totalNcp += difference;
								gain = gain.add(tuple._2._3());
								//totalUl = totalUl.add(tuple._2._3());
								remove.add(tuple._2._4());
								BigDecimal ratio = tuple._2._6().compareTo(BigDecimal.ZERO) != 0
									? new BigDecimal(ncpMerged).divide(tuple._2._6(), 200, RoundingMode.CEILING) : new BigDecimal("100000000000");
								mergedGroups.put(centerID, new Tuple4<>(ncpMerged, tuple._2._6(), mergedGroup, ratio));
								numOfGroups--;
								mergeCounter++;
								changes = true;
								break;
							}
						}
					}
				}
			}
			totalUl = totalUl.add(gain);

			if (mergeCounter < merges * 0.1)
			{
				if (treeSize < maxTreeSize)
				{
					treeSize++;
					changes = true;
				}
			} else if (treeSize > minTreeSize)
			{
				treeSize--;
			}

			if (iterationCounter < 9)
			{
				v = v.add(gain);
			} else if (iterationCounter == 9)
			{
				v = v.add(gain);
				v = v.divide(new BigDecimal("100"), 5, RoundingMode.DOWN);
				System.out.println("BOUND SET TO " + v.divide(this.meta.value().getTransactionSR().multiply(new BigDecimal(this.meta.value().getNumOfInstances())), 5, RoundingMode.CEILING));
			} else if (v.compareTo(gain) < 0)
			{
				System.out.println("EARLY TERMINATION FOR GAIN =" + gain.divide(this.meta.value().getTransactionSR().multiply(new BigDecimal(this.meta.value().getNumOfInstances())), 5, RoundingMode.CEILING));
				changes = false;
			}

			Broadcast<Set<Long>> brUsed = MySparkContext.getSparkContext().broadcast(remove);
			Broadcast<Map<Long, Tuple4<Double, BigDecimal, Group, BigDecimal>>> brMergedGroups = MySparkContext.getSparkContext().broadcast(mergedGroups);

			groups = groups.filter((Tuple5<Double, BigDecimal, Group, Long, BigDecimal> t1) -> !brUsed.value().contains(t1._4()));
			groups = groups.map((Tuple5<Double, BigDecimal, Group, Long, BigDecimal> t1)
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
			}).cache();
			if (iterationCounter % 100 == 99)
			{
				groups.checkpoint();
				groups.take(1);
			}
			iterationCounter++;
		} while (changes && totalNcp < this.meta.value().getRd() && totalUl.compareTo(this.meta.value().getTd()) > 0);
		input.unpersist(false);

		BigDecimal testerul= totalUl.divide(this.meta.value().getTransactionSR().multiply(new BigDecimal(this.meta.value().getNumOfInstances())),5, RoundingMode.CEILING).setScale(5, BigDecimal.ROUND_DOWN);
		double testerncp = totalNcp / (this.meta.value().getNumOfInstances() * this.meta.value().getNumOfAttributes());
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(5);
		df.setMinimumFractionDigits(5);
		String format = df.format(testerul);
		double time = (System.currentTimeMillis() - start) / 1000;
		System.out.println("Achilles " + String.format(format) + "," + String.format("%.2f", testerncp));

		JavaRDD<Group> result = groups.map((Tuple5<Double, BigDecimal, Group, Long, BigDecimal> v1) -> v1._3());
		return result;
	}

}
