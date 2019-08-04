package metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import struct.Details;
import struct.Hierarchy;
import struct.Instance;
import struct.MyTreeMap;
import struct.NumAttribute;

/**
 *
 * @author achilles
 */
public class NcpComparator implements PairFlatMapFunction<Iterator<Tuple2<Long, Instance>>, Instance, List<Tuple3<Long, Instance, Double>>>, Serializable
{

	private final Broadcast<Details> meta;
	private final Broadcast<List<Instance>> list;

	public NcpComparator(Broadcast<Details> details, Broadcast<List<Instance>> instance)
	{
		this.meta = details;
		this.list = instance;
	}

	@Override
	public Iterator<Tuple2<Instance, List<Tuple3<Long, Instance, Double>>>> call(Iterator<Tuple2<Long, Instance>> iterator)
	{
		List<MyTreeMap<Double, Tuple2<Long, Instance>>> treemaplist = new ArrayList<>();
		for (int i = 0; i < list.value().size(); i++)
		{
			treemaplist.add(new MyTreeMap<>(this.meta.value().getK()));
		}

		while (iterator.hasNext())
		{
			Tuple2<Long, Instance> tuple = iterator.next();
			Instance selected = tuple._2;
			List<Double> ncp = ncp(selected);
			for (int i = 0; i < ncp.size(); i++)
			{
				treemaplist.get(i).put(ncp.get(i), tuple);
			}
		}

		List<Tuple2<Instance, List<Tuple3<Long, Instance, Double>>>> tupleList = new ArrayList<>();
		for (int i = 0; i < list.value().size(); i++)
		{
			MyTreeMap<Double, Tuple2<Long, Instance>> treeMap = treemaplist.get(i);
			Instance instance = list.value().get(i);
			List<Tuple3<Long, Instance, Double>> tempList = new ArrayList<>();
			for (Entry<Double, ArrayList<Tuple2<Long, Instance>>> entry : treeMap.entrySet())
			{
				Double ncp = entry.getKey();
				for (Tuple2<Long, Instance> l : entry.getValue())
				{
					tempList.add(new Tuple3<>(l._1, l._2, ncp));
				}
			}
			Tuple2<Instance, List<Tuple3<Long, Instance, Double>>> t = new Tuple2<>(instance, tempList);
			tupleList.add(t);
		}
		return tupleList.iterator();
	}

	private List<Double> ncp(Instance b)
	{
		List<Double> resultList = new ArrayList<>();
		for (int i = 0; i < list.value().size(); i++)
		{
			Instance a = list.value().get(i);
			double result = 0;
			for (int j = 0; j < a.catSize(); j++)
			{
				Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(j);
				Integer v1 = a.getCat(j);
				Integer v2 = b.getCat(j);
				result += hierarchy.numOfLeaves(hierarchy.lca(v1, v2));
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
}
