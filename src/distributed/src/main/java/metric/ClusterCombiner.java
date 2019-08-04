package metric;

import algorithms.Apriori;
import algorithms.PCTA;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;
import struct.Details;
import struct.Group;
import struct.Hierarchy;
import struct.Instance;
import struct.MyTreeMap;
import struct.NumAttribute;
import struct.ULmethod;

/**
 *
 * @author achilles
 */
public class ClusterCombiner implements Serializable, PairFlatMapFunction<Iterator<Tuple5<Double, BigDecimal, Group, Long, BigDecimal>>, Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>>
{

	private final Broadcast<Details> meta;
	private final Broadcast<ArrayList<Tuple4<Double, BigDecimal, Group, Long>>> groups;
	private final Set<Long> centersID;
	private final BigInteger TWO;
	private final int treeSize;

	public ClusterCombiner(Broadcast<Details> meta, Broadcast<ArrayList<Tuple4<Double, BigDecimal, Group, Long>>> brCenters, int treeSize)
	{
		this.meta = meta;
		this.groups = brCenters;
		this.centersID = new HashSet<>();
		groups.value().forEach((value)
			-> 
			{
				centersID.add(value._4());
		});
		this.TWO = new BigInteger("2");
		this.treeSize = treeSize;
	}

	@Override
	public Iterator<Tuple2<Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>>> call(Iterator<Tuple5<Double, BigDecimal, Group, Long, BigDecimal>> t)
	{
		List<MyTreeMap<BigDecimal, Tuple6<Double, BigDecimal, Long, Double, BigDecimal, Group>>> list = new ArrayList<>();
		groups.value().forEach((_item)
			-> 
			{
				list.add(new MyTreeMap<>(1));
		});

		List<MyTreeMap<Double, Tuple5<Double, BigDecimal, Group, Long, BigDecimal>>> templist = new ArrayList<>();
		groups.value().forEach((_item)
			-> 
			{
				templist.add(new MyTreeMap<>(this.treeSize));
		});

		while (t.hasNext())
		{
			Tuple5<Double, BigDecimal, Group, Long, BigDecimal> next = t.next();
			if (!centersID.contains(next._4()))
			{
				for (int i = 0; i < list.size(); i++)
				{
					MyTreeMap<Double, Tuple5<Double, BigDecimal, Group, Long, BigDecimal>> get = templist.get(i);
					Tuple4<Double, BigDecimal, Group, Long> center = groups.value().get(i);
					Group centerGroup = center._3();
					Group selectedGroup = next._3();
					double ncpMerge = ncp(centerGroup, selectedGroup);
					double ncpDiff = ncpMerge - next._1() - center._1();
					get.put(ncpDiff, next);
				}
			}
		}

		for (int i = 0; i < templist.size(); i++)
		{
			Tuple4<Double, BigDecimal, Group, Long> center = groups.value().get(i);
			MyTreeMap<Double, Tuple5<Double, BigDecimal, Group, Long, BigDecimal>> maptemp = templist.get(i);
			Group centerGroup = center._3();
			MyTreeMap<BigDecimal, Tuple6<Double, BigDecimal, Long, Double, BigDecimal, Group>> map = list.get(i);
			maptemp.entrySet().forEach((entry)
				-> 
				{
					Double ncpDiff = entry.getKey();
					entry.getValue().forEach((next)
						-> 
						{
							Group selectedGroup = next._3();
							Group mergedGroup = mergeGroups(centerGroup, selectedGroup);
							BigDecimal mergedUl = ul(mergedGroup);
							BigDecimal ulDiff = mergedUl.subtract(center._2()).subtract(next._2());
							if (ulDiff.compareTo(BigDecimal.ZERO) == -1)
							{
								Double ncpMerge = ncp(mergedGroup);
								BigDecimal ncpDiffBD = new BigDecimal(ncpDiff);
								//utility gain /  ncp loss
								BigDecimal gain = ncpDiff != 0 ? ulDiff.divide(ncpDiffBD, 200, RoundingMode.CEILING) : new BigDecimal("100000000000");
								Tuple6<Double, BigDecimal, Long, Double, BigDecimal, Group> tuple = new Tuple6<>(ncpDiff, ulDiff, next._4(), ncpMerge, mergedUl, mergedGroup);
								map.put(gain, tuple);
							}
					});
			});
		}

		int counter = 0;
		List<Tuple2<Long, Tuple7<BigDecimal, Double, BigDecimal, Long, Double, BigDecimal, Group>>> result = new ArrayList<>();
		for (MyTreeMap<BigDecimal, Tuple6<Double, BigDecimal, Long, Double, BigDecimal, Group>> selected : list)
		{
			Long selectedCenter = groups.value().get(counter)._4();
			selected.entrySet().forEach((Map.Entry<BigDecimal, ArrayList<Tuple6<Double, BigDecimal, Long, Double, BigDecimal, Group>>> entry)
				-> 
				{
					BigDecimal key = entry.getKey();
					entry.getValue().forEach((v)
						-> 
						{

							result.add(new Tuple2<>(selectedCenter, new Tuple7<>(key, v._1(), v._2(), v._3(), v._4(), v._5(), v._6())));
					});
			});
			counter++;
		}
		return result.iterator();
	}

	public BigDecimal ul(Group group)
	{
		if (this.meta.value().getUlFunction().equals(ULmethod.pcta))
		{
			return ulPCTA(group);
		}

		if (this.meta.value().getUlFunction().equals(ULmethod.hierarchy))
		{
			return ulHierarchy(group);
		}
		return null;
	}

//	private BigDecimal ulPCTA(Group group)
//	{
//		PCTA pcta = new PCTA(this.meta);
//		Group output = pcta.call(group);
//		BigInteger result = BigInteger.ZERO;
//		Map<BitSet, Integer> map = new HashMap<>();
//		for (int i = 0; i < output.size(); i++)
//		{
//			Instance instance = output.getInstance(i);
//			List<BitSet> list = instance.getTraList();
//			for (BitSet itemset : list)
//			{
//				Integer get = map.get(itemset);
//				if (get == null)
//				{
//					map.put(itemset, 1);
//				} else
//				{
//					map.put(itemset, get + 1);
//				}
//			}
//		}
//		Iterator<Map.Entry<BitSet, Integer>> iterator = map.entrySet().iterator();
//		while (iterator.hasNext())
//		{
//			Map.Entry<BitSet, Integer> next = iterator.next();
//			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
//			BigInteger ul = ulPCTA(next.getKey()).multiply(multiplier);
//			result = result.add(ul);
//		}
//		return new BigDecimal(result);
//	}
//
//	private BigInteger ulPCTA(BitSet itemset)
//	{
//		int size = itemset.cardinality();
//		if (size > 1)
//		{
//			return TWO.pow(itemset.cardinality()).subtract(BigInteger.ONE);
//		} else
//		{
//			return BigInteger.ZERO;
//		}
//	}
//
//	private BigDecimal ulHierarchy(Group input)
//	{
//		Apriori apriori = new Apriori(meta);
//		Group output = apriori.call(input);
//		BigInteger result = BigInteger.ZERO;
//		Map<Integer, Integer> map = new HashMap<>();
//		for (int i = 0; i < output.size(); i++)
//		{
//			Instance instance = output.getInstance(i);
//			int bitCounter = -1;
//			BitSet items = instance.getTra();
//			while ((bitCounter = items.nextSetBit(bitCounter + 1)) != -1)
//			{
//				Integer get = map.get(bitCounter);
//				if (get == null)
//				{
//					map.put(bitCounter, 1);
//				} else
//				{
//					map.put(bitCounter, get + 1);
//				}
//			}
//		}
//
//		Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
//		while (iterator.hasNext())
//		{
//			Map.Entry<Integer, Integer> next = iterator.next();
//			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
//			BigInteger ul = ulHierarchy(next.getKey()).multiply(multiplier);
//			result = result.add(ul);
//		}
//		return new BigDecimal(result);
//	}
//
//	private BigInteger ulHierarchy(int item)
//	{
//		int u = this.meta.value().getTransactionHierarchy().numOfLeaves(item);
//		return TWO.pow(u).subtract(BigInteger.ONE);
//	}
	private BigDecimal ulPCTA(Group group)
	{
		PCTA pcta = new PCTA(this.meta);
		Group output = pcta.call(group);
		BigInteger result = BigInteger.ZERO;
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < output.size(); i++)
		{
			Instance instance = output.getInstance(i);
			List<BitSet> list = instance.getTraList();
			for (BitSet itemset : list)
			{
				int cardinality = itemset.cardinality();
				Integer get = map.get(cardinality);
				if (get == null)
				{
					map.put(cardinality, 1);
				} else
				{
					map.put(cardinality, get + 1);
				}
			}
		}
		Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext())
		{
			Map.Entry<Integer, Integer> next = iterator.next();
			int cardinality = next.getKey();
			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
			BigInteger ul = cardinality > 1 ? TWO.pow(cardinality).subtract(BigInteger.ONE).multiply(multiplier) : BigInteger.ZERO;
			result = result.add(ul);
		}
		return new BigDecimal(result);
	}

	private BigDecimal ulHierarchy(Group input)
	{
		Apriori apriori = new Apriori(meta);
		Group output = apriori.call(input);
		BigInteger result = BigInteger.ZERO;
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < output.size(); i++)
		{
			Instance instance = output.getInstance(i);
			int bitCounter = -1;
			BitSet items = instance.getTra();
			while ((bitCounter = items.nextSetBit(bitCounter + 1)) != -1)
			{
				Integer get = map.get(bitCounter);
				if (get == null)
				{
					map.put(bitCounter, 1);
				} else
				{
					map.put(bitCounter, get + 1);
				}
			}
		}

		Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext())
		{
			Map.Entry<Integer, Integer> next = iterator.next();
			int node = next.getKey();
			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
			BigInteger ul = this.meta.value().getTransactionHierarchy().numOfLeavesExp(node).multiply(multiplier);
			result = result.add(ul);
		}
		return new BigDecimal(result);
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

	private Double ncp(Group group)
	{
		Instance instance = group.getInstance(0);
		double ncp = ncp(instance);
		double result = ncp * group.size();
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
//				result += (double) temp / hierarchy.hierarchyLeaves();
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
