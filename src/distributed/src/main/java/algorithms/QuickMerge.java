package algorithms;

import java.io.IOException;
import java.io.Serializable;
import metric.NcpCalculator;
import metric.NcpGroupCalculator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import metric.QuickClusterCombiner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import struct.Details;
import struct.Group;
import struct.Hierarchy;
import struct.Instance;
import struct.MySparkContext;
import struct.MyTreeMap;
import struct.NumAttribute;

/**
 *
 * @author achilles
 */
public class QuickMerge implements Serializable
{

	private final Broadcast<Details> meta;

	public QuickMerge(Broadcast<Details> meta)
	{
		this.meta = meta;
	}

	public JavaRDD<Group> calc(JavaRDD<Group> input) throws IOException, InterruptedException
	{
		input.cache();
		NcpCalculator datasetCalc = new NcpCalculator(meta, input);
		double totalNcp = datasetCalc.ncp();
		System.out.println("Achilles initial ncp " + totalNcp);
		long numOfGroups = input.count();
		NcpGroupCalculator calc = new NcpGroupCalculator(meta);

		JavaRDD<Tuple3<Double, Group, Long>> groups = input.map(calc).zipWithUniqueId().map((Tuple2<Tuple2<Double, Group>, Long> t1)
			-> 
			{
				return new Tuple3<>(t1._1._1, t1._1._2, t1._2);
		});
		groups.cache();

		boolean changes;
		int merges = this.meta.value().getMaxMerges();
		int iterationCounter = 0;
		do
		{
			long time = System.currentTimeMillis();
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

			MyTreeMap<Double, Tuple2<Group, Long>> aggregate = groups.aggregate(new MyTreeMap<>(merges), (MyTreeMap<Double, Tuple2<Group, Long>> v1, Tuple3<Double, Group, Long> v2)
				-> 
				{
					v1.put(v2._1(), new Tuple2<>(v2._2(), v2._3()));
					return v1;
			}, (MyTreeMap<Double, Tuple2<Group, Long>> v1, MyTreeMap<Double, Tuple2<Group, Long>> v2)
				-> 
				{
					v2.entrySet().forEach((entry)
						-> 
						{
							entry.getValue().forEach((tuple2)
								-> 
								{
									v1.put(entry.getKey(), tuple2);
							});
					});
					return v1;
			});

			Collection<ArrayList<Tuple2<Group, Long>>> values = aggregate.values();
			ArrayList<Tuple2<Group, Long>> centers = new ArrayList<>();
			values.forEach((entry) -> centers.addAll(entry));

			Broadcast<ArrayList<Tuple2<Group, Long>>> brCenters = MySparkContext.getSparkContext().broadcast(centers);

			QuickClusterCombiner mergeFinder = new QuickClusterCombiner(meta, brCenters);
			JavaPairRDD<Long, Tuple3<Double, Long, Group>> reduceByKey = groups.mapPartitionsToPair(mergeFinder).reduceByKey((Tuple3<Double, Long, Group> v1, Tuple3<Double, Long, Group> v2)
				-> 
				{
					return Double.compare(v1._1(), v2._1()) < 0 ? v1 : v2;
			});
			List<Tuple2<Long, Tuple3<Double, Long, Group>>> match = reduceByKey.collect();

			Map<Long, Tuple2<Double, Group>> mergedGroups = new HashMap<>();
			Set<Long> remove = new HashSet<>();

			for (Tuple2<Long, Tuple3<Double, Long, Group>> tuple : match)
			{
				Long centerID = tuple._1();
				if (!remove.contains(tuple._2._2()))
				{
					for (Tuple2<Group, Long> temp : centers)
					{
						if (temp._2.equals(centerID))
						{
							Group centerGroup = temp._1;
							Group mergedGroup = mergeGroups(centerGroup, tuple._2._3());
							double ncp = ncp(mergedGroup);
							double difference = ncp - ncp(centerGroup) - ncp(tuple._2._3());
							if (totalNcp + difference < this.meta.value().getRd())
							{
								totalNcp += difference;
								remove.add(tuple._2._2());
								mergedGroups.put(centerID, new Tuple2<>(ncp, mergedGroup));
								numOfGroups--;
								changes = true;
								break;
							}
						}
					}
				}
			}

			Broadcast<Set<Long>> brRemove = MySparkContext.getSparkContext().broadcast(remove);
			Broadcast<Map<Long, Tuple2<Double, Group>>> brMergedGroups = MySparkContext.getSparkContext().broadcast(mergedGroups);

			groups = groups.filter((Tuple3<Double, Group, Long> t1)
				-> 
				{
					return !brRemove.value().contains(t1._3());
			}).map((Tuple3<Double, Group, Long> t1)
				-> 
				{
					Tuple2<Double, Group> get = brMergedGroups.value().get(t1._3());
					if (get == null)
					{
						return t1;
					} else
					{
						return new Tuple3<>(get._1, get._2, t1._3());
					}
			}
			);

			if (iterationCounter % 100 == 99)
			{
				groups.checkpoint();
			}
			iterationCounter++;

			long test = System.currentTimeMillis() - time;
			System.out.println("Achilles " + test + " " + totalNcp);
		} while (changes && totalNcp < this.meta.value().getRd());
		System.out.println("Achilles local ncp " + totalNcp);
		input.unpersist(false);
		JavaRDD<Group> result = groups.map((Tuple3<Double, Group, Long> v1) -> v1._2());
		return result;
	}

	private Instance merge(Instance a, Instance b)
	{
		Instance result = new Instance();
		for (int i = 0; i < a.catSize(); i++)
		{
			Integer n = this.meta.value().getCategoricalHierarchies().get(i).lca(a.getCat(i), b.getCat(i));
			result.add(n);
		}
		for (int i = 0; i < a.numSize(); i++)
		{
			NumAttribute temp = new NumAttribute(a.getNum(i));
			temp.extend(b.getNum(i));
			result.add(temp);
		}
		return result;
	}

	private Group mergeGroups(Group a, Group b)
	{
		Group group = new Group();
		group.setGeneralized(true);
		Instance mergedInstance = merge(a.getInstance(0), b.getInstance(0));
		addMerged(group, a, mergedInstance);
		addMerged(group, b, mergedInstance);
		return group;
	}

	private void addMerged(Group group, Group temp, Instance instance)
	{
		for (int i = 0; i < temp.size(); i++)
		{
			Instance clone = new Instance(instance);
			clone.getTra().clear();
			clone.add(temp.getInstance(i).getTra());
			group.addInstance(clone);
		}
	}

	private Double ncp(Group group)
	{
		Instance instance = group.getInstance(0);
		double ncp = ncp(instance);
		double result = ncp * group.size() / this.meta.value().getNcpDiv();
		return result;
	}

	private double ncp(Instance e)
	{
		double result = 0;
		for (int i = 0; i < e.catSize(); i++)
		{
			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(i);
			result += hierarchy.numOfLeaves(e.getCat(i));
		}

		for (int i = 0; i < e.numSize(); i++)
		{
			double range = e.getNum(i).range();
			if (range > 0)
			{
				result += range / this.meta.value().getRanges().get(i).range();
			}
		}
		return result;
	}
//	private double ncp(Instance e)
//	{
//		double result = 0;
//		for (int i = 0; i < e.catSize(); i++)
//		{
//			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(i);
//			Integer temp = hierarchy.numOfLeaves(e.getCat(i));
//			if (temp > 1)
//			{
//				result += temp / hierarchy.hierarchyLeaves();
//			}
//		}
//
//		for (int i = 0; i < e.numSize(); i++)
//		{
//			double range = e.getNum(i).range();
//			if (range > 0)
//			{
//				result += range / this.meta.value().getRanges().get(i).range();
//			}
//		}
//		return result;
//	}

}
