package algorithms;

import metric.NcpComparator;
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
import struct.Details;
import struct.Group;
import struct.Instance;
import struct.MySparkContext;

/**
 *
 * @author achilles
 */
public class ClusterTesting implements Serializable
{

	private JavaPairRDD<Long, Instance> dataset;
	private final Broadcast<Details> meta;

	public ClusterTesting(JavaPairRDD<Long, Instance> dataset, Broadcast<Details> details)
	{
		this.dataset = dataset;
		this.meta = details;
	}

	public JavaRDD<Group> createClusters(int maxc)
	{
		JavaRDD<Group> groups = MySparkContext.getSparkContext().emptyRDD();
		List<Group> groupList = new ArrayList<>();
		JavaPairRDD<Long, Instance> prevdataset;
		JavaRDD<Group> prevgroups;
		int i = 0;
		int maxClusters = maxc;
		int clusters = maxClusters;
		int k = meta.value().getK();
		dataset.cache();
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
			dataset.count();
			prevdataset.unpersist(false);
			i++;
		} while (false);

		return groups;
	}

}
