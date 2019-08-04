package algorithms;

import metric.NcpComparator;
import metric.NcpComparatorRemaining;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import struct.Anon;
import struct.Details;
import struct.Group;
import struct.Instance;
import struct.MySparkContext;

/**
 *
 * @author achilles
 */
public class Cluster implements Serializable
{

	private JavaPairRDD<Long, Instance> dataset;
	private final Broadcast<Details> meta;

	public Cluster(JavaPairRDD<Long, Instance> dataset, Broadcast<Details> details)
	{
		this.dataset = dataset;
		this.meta = details;
	}

	public JavaRDD<Group> createClusters()
	{
		JavaRDD<Group> groups = MySparkContext.getSparkContext().emptyRDD();
		List<Group> groupList = new ArrayList<>();
		JavaPairRDD<Long, Instance> prevdataset;
		JavaRDD<Group> prevgroups;
		int i = 0;
		int maxClusters = this.meta.value().getMaxClusters();
		int clusters = maxClusters;
		int k = meta.value().getK();
		dataset.cache();
		long size;
		final int totalSize = k + this.meta.value().getMaxConflicts();
		do
		{
			List<Tuple2<Long, Instance>> selectedTuples = this.dataset.take(clusters);
			List<Instance> list = new ArrayList<>();
			for (Tuple2<Long, Instance> t : selectedTuples)
			{
				Instance temp = t._2;
				temp.getTra().clear();
				list.add(temp);
			}
			Broadcast<List<Instance>> centers = MySparkContext.getSparkContext().broadcast(list);
			NcpComparator comp = new NcpComparator(meta, centers);
			JavaPairRDD<Instance, List<Tuple3<Long, Instance, Double>>> candidates = this.dataset.mapPartitionsToPair(comp)
				.reduceByKey((List<Tuple3<Long, Instance, Double>> t1, List<Tuple3<Long, Instance, Double>> t2)
					-> 
					{
						List<Tuple3<Long, Instance, Double>> result = new ArrayList<>();
						boolean empty1 = t1.isEmpty();
						boolean empty2 = t2.isEmpty();
						while (result.size() < totalSize && !(empty1 && empty2))
						{
							if (!empty1 && !empty2)
							{
								if (t1.get(0)._3() < t2.get(0)._3())
								{
									Tuple3<Long, Instance, Double> removed = t1.remove(0);
									empty1 = t1.isEmpty();
									if (!result.contains(removed))
									{
										result.add(removed);
									}
								} else
								{
									Tuple3<Long, Instance, Double> removed = t2.remove(0);
									empty2 = t2.isEmpty();
									if (!result.contains(removed))
									{
										result.add(removed);
									}
								}
							} else if (empty1)
							{
								Tuple3<Long, Instance, Double> removed = t2.remove(0);
								empty2 = t2.isEmpty();
								if (!result.contains(removed))
								{
									result.add(removed);
								}
							} else
							{
								Tuple3<Long, Instance, Double> removed = t1.remove(0);
								empty1 = t1.isEmpty();
								if (!result.contains(removed))
								{
									result.add(removed);
								}
							}
						}
						return result;
				});
			List<Tuple2<Instance, List<Tuple3<Long, Instance, Double>>>> collect = candidates.collect();
			centers.destroy(false);

			Set<Long> used = new HashSet<>();
			int numOfGroupsCreated = 0;
			for (int q = 0; q < collect.size(); q++)
			{
				List<Tuple3<Long, Instance, Double>> g = collect.get(q)._2;
				Iterator<Tuple3<Long, Instance, Double>> it = g.iterator();
				int counter = 0;
				while (it.hasNext())
				{
					Tuple3<Long, Instance, Double> next = it.next();
					if (used.contains(next._1()) || counter == k)
					{
						it.remove();
					} else
					{
						counter++;
					}
				}
				if (g.size() == k)
				{
					Group newGroup = new Group();
					for (Tuple3<Long, Instance, Double> tuple : g)
					{
						used.add(tuple._1());
						newGroup.addInstance(tuple._2());
					}
					groupList.add(newGroup);
					numOfGroupsCreated++;
				}
			}

			Broadcast<Set<Long>> ids = MySparkContext.getSparkContext().broadcast(used);

			prevdataset = this.dataset;
			this.dataset = this.dataset.filter((Tuple2<Long, Instance> v1)
				-> 
				{
					return !ids.value().contains(v1._1);
			}).cache();
			size = dataset.count();
			prevdataset.unpersist(false);
			if (i % 400 == 399)
			{
				this.dataset.cache();
				this.dataset.checkpoint();
			}

			if (i % 10 == 9)
			{
				JavaRDD<Group> groupRDD = MySparkContext.getSparkContext().parallelize(groupList);
				prevgroups = groups;
				groups = groups.union(groupRDD).cache();
				if (i % 400 == 399)
				{
					groups.checkpoint();
				}
				groups.take(1);
				prevgroups.unpersist(false);
				groupList.clear();
			}

			if (numOfGroupsCreated < clusters * 0.2)
			{
				clusters = clusters / 2;
			} else if (numOfGroupsCreated == clusters && clusters <= maxClusters)
			{
				clusters++;
			} else
			{
				clusters--;
			}

			i++;
		} while (size >= k);

		prevgroups = groups;
		if (!groupList.isEmpty())
		{
			JavaRDD<Group> groupRDD = MySparkContext.getSparkContext().parallelize(groupList);
			groups = groups.union(groupRDD);
		}

		JavaPairRDD<Long, Group> pairGroups = groups.zipWithUniqueId().mapToPair((Tuple2<Group, Long> v1) -> new Tuple2<>(v1._2, v1._1)).coalesce(this.meta.value().getNumOfPartitions(), false).cache();
		JavaPairRDD<Long, Group> prevpairGroups = pairGroups;
		prevgroups.unpersist(false);

		if (!this.dataset.isEmpty())
		{
			List<Tuple2<Long, Instance>> remainingList = this.dataset.collect();
			Broadcast<List<Tuple2<Long, Instance>>> brRemaining = MySparkContext.getSparkContext().broadcast(remainingList);
			NcpComparatorRemaining compRemaining = new NcpComparatorRemaining(meta, brRemaining);
			JavaPairRDD<Long, Tuple3<Instance, Long, Double>> candidates = pairGroups.mapPartitionsToPair(compRemaining);
			JavaPairRDD<Long, Tuple3<Instance, Long, Double>> matches = candidates.reduceByKey((Tuple3<Instance, Long, Double> v1, Tuple3<Instance, Long, Double> v2)
				-> 
				{
					return Double.compare(v1._3(), v2._3()) < 0 ? v1 : v2;
			});
			List<Tuple2<Long, Tuple3<Instance, Long, Double>>> collect = matches.collect();
			brRemaining.destroy(false);
			Broadcast<List<Tuple2<Long, Tuple3<Instance, Long, Double>>>> brBestMatches = MySparkContext.getSparkContext().broadcast(collect);
			pairGroups = pairGroups.mapToPair((Tuple2< Long, Group> t)
				-> 
				{
					List<Instance> list = new ArrayList<>();
					for (Tuple2<Long, Tuple3<Instance, Long, Double>> tuple : brBestMatches.value())
					{
						if (java.util.Objects.equals(t._1, tuple._2._2()))
						{
							list.add(tuple._2._1());
						}
					}
					if (list.isEmpty())
					{
						return t;
					} else
					{
						Group g = new Group(t._2);
						list.forEach((e)
							-> 
							{
								g.addInstance(e);
						});
						return new Tuple2<>(t._1, g);
					}
			});
		}

		this.dataset.unpersist(false);
		Anon anon = new Anon(this.meta);
		JavaRDD<Group> result = pairGroups.map(anon).mapToPair((Group v1)
			-> 
			{
				Instance e = new Instance(v1.getInstance(0));
				e.getTra().clear();
				return new Tuple2<>(e, v1);
		}).reduceByKey((Group v1, Group v2)
			-> 
			{
				Group g = new Group();
				for (int j = 0; j < v1.size(); j++)
				{
					g.addInstance(v1.getInstance(j));
				}
				for (int j = 0; j < v2.size(); j++)
				{
					g.addInstance(v2.getInstance(j));
				}
				return g;
		}).map((Tuple2<Instance, Group> v1) -> v1._2);
		prevpairGroups.unpersist(false);
		return result;
	}

}
