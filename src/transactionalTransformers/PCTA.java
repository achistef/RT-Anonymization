package transactionalTransformers;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import structures.Group;
import structures.Instance;

/**
 *
 * @author achilles
 */
public class PCTA
{

	private final Group group;
	private final int k;
	private final int m;
	private final Queue<List<Set<Integer>>> pq;
	private final Set<BitSet> constraints;
	private final HashMap<Integer, Set<Integer>> H;

	public PCTA(Group group, int k, int m)
	{
		this.pq = new PriorityQueue<>(new ConstraintComperator());
		this.H = new HashMap<>();
		this.constraints = new HashSet<>();
		this.group = group;
		this.k = k;
		this.m = m;
		Set<Integer> nodes = findNodes();
		nodes.forEach ((integer) -> {
			Set<Integer> set = new HashSet<>();
			set.add(integer);
			this.H.put(integer, set);
		});
		//calcCountTree();
		generateConstraints();
		//System.out.println("found nodes : " + this.H.size());
		//System.out.println("generated constraints : " + this.pq.size());
	}

	public void pcta()
	{
		while (!this.pq.isEmpty())
		{
			List<Set<Integer>> p = this.pq.peek();
			int size = p.size();
			for (int i = 0; i < size; i++)
			{
				Set<Integer> set = p.get(i);
				if (set.size() != 1)
				{
					System.out.println("set contains more than one item!");
				}
				Integer item = set.iterator().next();
				if (!equalsHash(item))
				{
					Set<Integer> value = this.H.get(item);
					if (!p.contains(value))
					{
						p.add(value);
					}
					p.remove(set);
					i--;
					size--;
				}
			}
			if (support(p) != 0)
			{
				while (support(p) < this.k)
				{
					long min = Long.MAX_VALUE;
					Set<Integer> im = null;
					Set<Integer> is = null;
					for (Set<Integer> set : p)
					{
						Set<Integer> bestMatch = findBestMatch(set);
						if (bestMatch == null)
						{
							break;
						}
						long ul = ul(generalize(set, bestMatch));
						if (ul < min)
						{
							min = ul;
							im = set;
							is = bestMatch;
						}
					}
					if (im == null || is == null)
					{
						//System.out.println("could not anonymize");
						break;
					}
					Set<Integer> generalized = generalize(im, is);
					p.remove(im);
					p.remove(is);
					p.add(generalized);
					replaceHash(generalized);

				}
			}
			this.pq.poll();
		}

	}

	public void applyRules()
	{
		for (int i = 0; i < this.group.size(); i++)
		{
			HashSet<BitSet> set = new HashSet<>();
			Instance instance = this.group.getInstance(i);
			BitSet bitset = instance.getTra();
			int bitCounter = bitset.nextSetBit(0);
			while (bitCounter != -1)
			{
				Set<Integer> generalized = this.H.get(bitCounter);
				BitSet temp = new BitSet();
				generalized.forEach ((item) -> {
					temp.set(item);
				});
				set.add(temp);
				bitCounter = bitset.nextSetBit(bitCounter + 1);
			}
			instance.getTra().clear();
			instance.getTraList().addAll(set);
		}
	}

	private void replaceHash(Set<Integer> itemset)
	{
		itemset.forEach ((item) -> {
			this.H.put(item, itemset);
		});
	}

	private Set<Integer> generalize(Set<Integer> itemset1, Set<Integer> itemset2)
	{
		Set<Integer> result = new HashSet<>();
		result.addAll(itemset1);
		result.addAll(itemset2);
		return result;
	}

	private long ul(Set<Integer> itemset)
	{
		List<Set<Integer>> list = new ArrayList<>();
		list.add(itemset);
		return ((long) Math.pow(2, itemset.size()) - 1) * support(list);
	}

	private Set<Integer> findBestMatch(Set<Integer> set)
	{
		Set<Integer> result = null;
		long ul = Long.MAX_VALUE;
		for (Set<Integer> value : this.H.values())
		{
			if (!set.equals(value))
			{
				Set<Integer> temp = new HashSet<>();
				temp.addAll(set);
				temp.addAll(value);
				long ulTemp = ul(temp);
				if (ulTemp < ul)
				{
					ul = ulTemp;
					result = value;
				}
			}
		}
		return result;
	}

	private int support(List<Set<Integer>> shell)
	{
		int support = 0;
		for (int i = 0; i < this.group.size(); i++)
		{
			BitSet instance = this.group.getInstance(i).getTra();
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
	
	private int support2(List<Set<Integer>> shell)
	{
		int support = 0;
		for (int i = 0; i < this.group.size(); i++)
		{
			BitSet instance = this.group.getInstance(i).getTra();
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

	/*private int support(List<Set<Integer>> shell)
	{
		int result = 0;
		int size = shell.size();
		BitSet bitSet = new BitSet();
		for (int i = 0; i < size; i++)
		{
			bitSet.set(i);
		}
		for (int i = 1; i < size + 1; i++)
		{
			List<BitSet> combinations = combinations(bitSet, i);
			int setSupport = 0;
			for (BitSet b : combinations)
			{
				Set<Integer> set = new HashSet<>();
				int bitCounter = -1;
				while ((bitCounter = b.nextSetBit(bitCounter + 1)) != -1)
				{
					for (Integer item : shell.get(bitCounter))
					{
						set.add(item);
					}
				}
				setSupport += setSupport(set);
			}
			result = (i % 2 == 1) ? result + setSupport : result - setSupport;
		}
		return result;
	}*/

 /*private int setSupport(Set<Integer> set)
	{
		if (set.size() > this.treeDepth)
		{
			System.out.println("tree is not supporting " + set.size() + "-itemsets");
			return Integer.MAX_VALUE;
		}
		int result = 0;
		int size = set.size();
		BitSet bitset = new BitSet();
		for (Integer item : set)
		{
			bitset.set(item);
		}
		for (int i = 1; i < size + 1; i++)
		{
			int setSupport = 0;
			List<BitSet> combinations = combinations(bitset, i);
			for (BitSet b : combinations)
			{
				setSupport += this.tree.support(b);
			}
			result = (i % 2 == 1) ? result + setSupport : result - setSupport;
		}
		return result;
	}*/
	private boolean equalsHash(Integer item)
	{
		Set<Integer> set = this.H.get(item);
		if (set.size() != 1)
		{
			return false;
		}
		Integer itemHash = set.iterator().next();
		return item.equals(itemHash);
	}

	private Set<Integer> findNodes()
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

	/*private void calcCountTree()
	{
		for (int j = 0; j < group.size(); j++)
		{
			BitSet bitset = group.getInstance(j).getTra();
			int size = Math.min(this.treeDepth, bitset.cardinality());
			for (int l = 1; l < size + 1; l++)
			{
				List<BitSet> combinations = combinations(bitset, l);
				for (BitSet bitSet : combinations)
				{
					this.tree.add(bitSet);
				}
			}
		}

	}*/
	private void generateConstraints()
	{
		for (int i = 0; i < this.group.size(); i++)
		{
			BitSet bitset = this.group.getInstance(i).getTra();
			List<BitSet> list = combinations(bitset);
			for (BitSet b : list)
			{
				if (!this.constraints.contains(b))
				{
					this.constraints.add(b);
					List<Set<Integer>> shell = new ArrayList<>();
					int bitCounter = -1;
					while ((bitCounter = b.nextSetBit(bitCounter + 1)) != -1)
					{
						Set<Integer> set = new HashSet<>();
						set.add(bitCounter);
						shell.add(set);
					}
					this.pq.add(shell);
				}
			}
		}
	}

	private List<BitSet> combinations(BitSet bitset)
	{
		ArrayList<BitSet> list = new ArrayList<>();
		int mFixed = Math.min(this.m, bitset.cardinality());
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

		@Override
		public int compare(List<Set<Integer>> o1, List<Set<Integer>> o2)
		{
			return Long.compare(support(o1), support(o2));
		}

	}

}
