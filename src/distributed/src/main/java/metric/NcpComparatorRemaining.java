package metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
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
public class NcpComparatorRemaining implements PairFlatMapFunction<Iterator<Tuple2<Long, Group>>, Long, Tuple3< Instance, Long, Double>>, Serializable
{

	private final Broadcast<Details> meta;
	private final Broadcast<List<Tuple2<Long, Instance>>> instances;

	public NcpComparatorRemaining(Broadcast<Details> details, Broadcast<List<Tuple2<Long, Instance>>> instance)
	{
		this.meta = details;
		this.instances = instance;
	}

	@Override
	public Iterator<Tuple2<Long, Tuple3< Instance, Long, Double>>> call(Iterator<Tuple2<Long, Group>> iterator) throws Exception
	{
		List<MyTreeMap<Double, Long>> list = new ArrayList<>();
		for (int i = 0; i < instances.value().size(); i++)
		{
			list.add(new MyTreeMap<>(1));
		}

		while (iterator.hasNext())
		{
			Tuple2<Long, Group> tuple = iterator.next();
			Group selected = tuple._2;
			List<Double> ncp = ncp(selected);
			for (int i = 0; i < ncp.size(); i++)
			{
				list.get(i).put(ncp.get(i), tuple._1);
			}
		}

		List<Tuple2<Long, Tuple3< Instance, Long, Double>>> tupleList = new ArrayList<>();
		for (int i = 0; i < list.size(); i++)
		{
			MyTreeMap<Double, Long> map = list.get(i);
			Entry<Double, Long> entry = map.pollFirstElement();
			if (entry != null)
			{
				Long a = instances.value().get(i)._1;
				Instance b = instances.value().get(i)._2;
				Long c = entry.getValue();
				Double d = entry.getKey();
				tupleList.add(new Tuple2<>(a, new Tuple3<>(b, c, d)));
			}
		}
		return tupleList.iterator();
	}

	private List<Double> ncp(Group g)
	{
		List<Double> resultList = new ArrayList<>();
		Instance b = generalize(g);

		for (int i = 0; i < instances.value().size(); i++)
		{
			Instance a = instances.value().get(i)._2;
			double result = 0;
			for (int j = 0; j < a.catSize(); j++)
			{
				Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(j);
				result += hierarchy.numOfLeaves(hierarchy.lca(a.getCat(j), b.getCat(j)));
			}
			for (int j = 0; j < a.numSize(); j++)
			{
				NumAttribute numAttribute = new NumAttribute(a.getNum(j));
				numAttribute.extend(b.getNum(j));
				double range = numAttribute.range();
				if (range > 0)
				{
					result += range / this.meta.value().getRanges().get(j).range();
				}
			}
			resultList.add(result);
		}
		return resultList;
	}

//	private List<Double> ncp(Group g)
//	{
//		List<Double> resultList = new ArrayList<>();
//		Instance b = generalize(g);
//
//		for (int i = 0; i < instances.value().size(); i++)
//		{
//			Instance a = instances.value().get(i)._2;
//			double result = 0;
//			for (int j = 0; j < a.catSize(); j++)
//			{
//				Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(j);
//				int temp = hierarchy.numOfLeaves(hierarchy.lca(a.getCat(j), b.getCat(j)));
//				if (temp > 1)
//				{
//					result += (double)temp / hierarchy.hierarchyLeaves();
//				}
//			}
//			for (int j = 0; j < a.numSize(); j++)
//			{
//				NumAttribute numAttribute = new NumAttribute(a.getNum(j));
//				numAttribute.extend(b.getNum(j));
//				double range = numAttribute.range();
//				if (range > 0)
//				{
//					result += range / this.meta.value().getRanges().get(j).range();
//				}
//			}
//			resultList.add(result);
//		}
//		return resultList;
//	}
	private Instance generalize(Group group)
	{
		Instance instance = new Instance(group.getInstance(0));

		for (int i = 1; i < group.size(); i++)
		{
			Instance temp = group.getInstance(i);
			for (int j = 0; j < instance.numSize(); j++)
			{
				instance.getNum(j).extend(temp.getNum(j));
			}

			for (int j = 0; j < instance.catSize(); j++)
			{
				int lca = this.meta.value().getCategoricalHierarchies().get(j).lca(instance.getCat(j), temp.getCat(j));
				instance.set(j, lca);
			}
		}
		return instance;
	}
}
