package metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import struct.Details;
import struct.Group;
import struct.Hierarchy;
import struct.Instance;
import struct.MyTreeMap;
import struct.NumAttribute;

/**
 *
 * @author achilles
 */
public class QuickClusterCombiner implements Serializable, PairFlatMapFunction<Iterator<Tuple3<Double, Group, Long>>, Long, Tuple3<Double, Long, Group>>
{

	private final Broadcast<Details> meta;
	private final Broadcast<ArrayList<Tuple2<Group, Long>>> groups;
	private final Set<Long> centersID;

	public QuickClusterCombiner(Broadcast<Details> meta, Broadcast<ArrayList<Tuple2<Group, Long>>> brCenters)
	{
		this.meta = meta;
		this.groups = brCenters;
		this.centersID = new HashSet<>();
		groups.value().forEach((value)
			-> 
			{
				centersID.add(value._2);
		});
	}

	@Override
	public Iterator<Tuple2<Long, Tuple3<Double, Long, Group>>> call(Iterator<Tuple3<Double, Group, Long>> t) throws Exception
	{

		List<MyTreeMap<Double, Tuple2<Long, Group>>> list = new ArrayList<>();
		groups.value().forEach((_item)
			-> 
			{
				list.add(new MyTreeMap<>(1));
		});

		while (t.hasNext())
		{
			Tuple3<Double, Group, Long> next = t.next();
			if (!centersID.contains(next._3()))
			{
				for (int i = 0; i < list.size(); i++)
				{
					MyTreeMap<Double, Tuple2<Long, Group>> map = list.get(i);
					Tuple2<Group, Long> center = groups.value().get(i);
					double ncp = ncp(next._2(), center._1) - next._1();
					map.put(ncp, new Tuple2<>(next._3(), next._2()));
				}
			}
		}

		int counter = 0;
		List<Tuple2<Long, Tuple3<Double, Long, Group>>> result = new ArrayList<>();
		for (MyTreeMap<Double, Tuple2<Long, Group>> selected : list)
		{
			Long selectedCenter = groups.value().get(counter)._2();
			selected.entrySet().forEach((entry)
				-> 
				{
					Double ncp = entry.getKey();
					entry.getValue().forEach((value)
						-> 
						{
							result.add(new Tuple2<>(selectedCenter, new Tuple3<>(ncp, value._1, value._2)));
					});
			});
			counter++;
		}
		return result.iterator();
	}

	private double ncp(Group a, Group b)
	{
		double ncp = ncp(a.getInstance(0), b.getInstance(0));
		int size = a.size() + b.size();
		return ncp * size;
	}

	private double ncp(Instance a, Instance b)
	{
		double result = 0;
		for (int j = 0; j < b.catSize(); j++)
		{
			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(j);
			result += hierarchy.numOfLeaves(hierarchy.lca(b.getCat(j), a.getCat(j)));
		}
		for (int j = 0; j < b.numSize(); j++)
		{
			NumAttribute numAttribute = new NumAttribute(b.getNum(j));
			numAttribute.extend(a.getNum(j));
			double range = numAttribute.range();
			if (range > 0)
			{
				result += range / this.meta.value().getRanges().get(j).range();
			}
		}
		return result;
	}

//	private double ncp(Instance a, Instance b)
//	{
//		double result = 0;
//		for (int j = 0; j < b.catSize(); j++)
//		{
//			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(j);
//			int temp = hierarchy.numOfLeaves(hierarchy.lca(b.getCat(j), a.getCat(j)));
//			if (temp > 1)
//			{
//				result += (double) temp / hierarchy.hierarchyLeaves();
//			}
//		}
//		for (int j = 0; j < b.numSize(); j++)
//		{
//			NumAttribute numAttribute = new NumAttribute(b.getNum(j));
//			numAttribute.extend(a.getNum(j));
//			double range = numAttribute.range();
//			if (range > 0)
//			{
//				result += range / this.meta.value().getRanges().get(j).range();
//			}
//		}
//		return result;
//	}
}
