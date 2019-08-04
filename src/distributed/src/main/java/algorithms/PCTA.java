package algorithms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import struct.Details;
import struct.Group;
import struct.Instance;

/**
 *
 * @author achilles
 */
public class PCTA implements Serializable, Function<Group, Group>
{

	private final int k;
	private final int m;
	private final Broadcast<Details> meta;

	public PCTA(Broadcast<Details> meta)
	{
		this.meta = meta;
		this.k = this.meta.value().getK();
		this.m = this.meta.value().getM();
	}

	@Override
	public Group call(Group input)
	{
		Group group = new Group(input);
		Queue<List<Set<Integer>>> pq = new PriorityQueue<>(new ConstraintComperator(group));
		HashMap<Integer, Set<Integer>> H = new HashMap<>();
		Set<BitSet> constraints = new HashSet<>();
		Set<Integer> nodes = findNodes(group);
		for (Integer integer : nodes)
		{
			Set<Integer> set = new HashSet<>();
			set.add(integer);
			H.put(integer, set);
		}
		generateConstraints(group, pq, constraints);
		while (!pq.isEmpty())
		{
			List<Set<Integer>> p = pq.peek();
			int size = p.size();
			for (int i = 0; i < size; i++)
			{
				Set<Integer> set = p.get(i);
				Integer item = set.iterator().next();
				if (!equalsHash(item, H))
				{
					Set<Integer> value = H.get(item);
					if (!p.contains(value))
					{
						p.add(value);
					}
					p.remove(set);
					i--;
					size--;
				}
			}
			if (support(p, group) != 0)
			{
				while (support(p, group) < this.k)
				{
					long min = Long.MAX_VALUE;
					Set<Integer> im = null;
					Set<Integer> is = null;
					for (Set<Integer> set : p)
					{
						Set<Integer> bestMatch = findBestMatch(set, H, group);
						if (bestMatch == null)
						{
							break;
						}
						long ul = ul(generalize(set, bestMatch), group);
						if (ul < min)
						{
							min = ul;
							im = set;
							is = bestMatch;
						}
					}
					if (im == null || is == null)
					{
						break;
					}
					Set<Integer> generalized = generalize(im, is);
					p.remove(im);
					p.remove(is);
					p.add(generalized);
					replaceHash(generalized, H);

				}
			}
			pq.poll();
		}
		applyRules(group, H);
		return group;
	}

	public void applyRules(Group group, HashMap<Integer, Set<Integer>> H)
	{
		for (int i = 0; i < group.size(); i++)
		{
			HashSet<BitSet> set = new HashSet<>();
			Instance instance = group.getInstance(i);
			BitSet bitset = instance.getTra();
			int bitCounter = bitset.nextSetBit(0);
			while (bitCounter != -1)
			{
				Set<Integer> generalized = H.get(bitCounter);
				BitSet temp = new BitSet();
				for (Integer item : generalized)
				{
					temp.set(item);
				}
				set.add(temp);
				bitCounter = bitset.nextSetBit(bitCounter + 1);
			}
			instance.getTra().clear();
			instance.getTraList().addAll(set);
		}
	}

	private void replaceHash(Set<Integer> itemset, HashMap<Integer, Set<Integer>> H)
	{
		for (Integer item : itemset)
		{
			H.put(item, itemset);
		}
	}

	private Set<Integer> generalize(Set<Integer> itemset1, Set<Integer> itemset2)
	{
		Set<Integer> result = new HashSet<>();
		result.addAll(itemset1);
		result.addAll(itemset2);
		return result;
	}

	private long ul(Set<Integer> itemset, Group group)
	{
		List<Set<Integer>> list = new ArrayList<>();
		list.add(itemset);
		return ((long) Math.pow(2, itemset.size()) - 1) * support(list, group);
	}

	private Set<Integer> findBestMatch(Set<Integer> set, HashMap<Integer, Set<Integer>> H, Group group)
	{
		Set<Integer> result = null;
		long ul = Long.MAX_VALUE;
		for (Set<Integer> value : H.values())
		{
			if (!set.equals(value))
			{
				Set<Integer> temp = new HashSet<>();
				temp.addAll(set);
				temp.addAll(value);
				long ulTemp = ul(temp, group);
				if (ulTemp < ul)
				{
					ul = ulTemp;
					result = value;
				}
			}
		}
		return result;
	}

	private int support(List<Set<Integer>> shell, Group group)
	{
		int support = 0;
		for (int i = 0; i < group.size(); i++)
		{
			BitSet instance = group.getInstance(i).getTra();
			boolean contains = true;
			for (Set<Integer> set : shell)
			{
				boolean partial = false;
				for (Integer item : set)
				{
					if (instance.get(item))
					{
						partial = true;
						break;
					}
				}
				if (!partial)
				{
					contains = false;
					break;
				}
			}
			if (contains)
			{
				support++;
			}
		}
		return support;
	}

	private boolean equalsHash(Integer item, HashMap<Integer, Set<Integer>> H)
	{
		Set<Integer> set = H.get(item);
		if (set.size() != 1)
		{
			return false;
		}
		Integer itemHash = set.iterator().next();
		return item.equals(itemHash);
	}

	private Set<Integer> findNodes(Group group)
	{
		Set<Integer> result = new HashSet<>();
		for (int j = 0; j < group.size(); j++)
		{
			Instance instance = group.getInstance(j);
			BitSet bitset = instance.getTra();
			int bitCounter = -1;
			while ((bitCounter = bitset.nextSetBit(bitCounter + 1)) != -1)
			{
				result.add(bitCounter);
			}
		}

		return result;
	}

	private void generateConstraints(Group group, Queue<List<Set<Integer>>> pq, Set<BitSet> constraints)
	{
		for (int i = 0; i < group.size(); i++)
		{
			BitSet bitset = group.getInstance(i).getTra();
			List<BitSet> list = combinations(bitset);
			for (BitSet b : list)
			{
				if (!constraints.contains(b))
				{
					constraints.add(b);
					List<Set<Integer>> shell = new ArrayList<>();
					int bitCounter = -1;
					while ((bitCounter = b.nextSetBit(bitCounter + 1)) != -1)
					{
						Set<Integer> set = new HashSet<>();
						set.add(bitCounter);
						shell.add(set);
					}
					pq.add(shell);
				}
			}
		}

	}

	private List<BitSet> combinations(BitSet bitset)
	{
		ArrayList<BitSet> list = new ArrayList<>();
		int mFixed = Math.min(m, bitset.cardinality());
		if (mFixed == 0)
		{
			return list;
		}
		int[] index = new int[mFixed];
		index[0] = bitset.nextSetBit(0);
		for (int i = 1; i < mFixed; i++)
		{
			index[i] = bitset.nextSetBit(index[i - 1] + 1);
		}
		while (index[0] != -1)
		{
			BitSet comb = new BitSet();
			for (int i = 0; i < index.length; i++)
			{
				comb.set(index[i]);
			}
			list.add(comb);

			int c = mFixed - 1;
			boolean traceback = false;
			do
			{
				if (traceback)
				{
					index[c] = bitset.nextSetBit(index[c - 1] + 1);
				} else
				{
					index[c] = bitset.nextSetBit(index[c] + 1);
				}

				if (index[c] == -1)
				{
					c--;
					traceback = false;
				} else
				{
					c++;
					traceback = true;
				}
			} while (c > -1 && c < mFixed);
		}
		return list;
	}

	private class ConstraintComperator implements Comparator<List<Set<Integer>>>
	{

		private final Group group;

		public ConstraintComperator(Group group)
		{
			this.group = group;
		}

		@Override
		public int compare(List<Set<Integer>> o1, List<Set<Integer>> o2)
		{
			return Long.compare(support(o1, group), support(o2, group));
		}

	}

}
