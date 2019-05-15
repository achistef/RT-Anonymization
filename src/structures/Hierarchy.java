package structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author achilles
 */
public class Hierarchy implements Serializable
{

	private final HashMap<Integer, Integer> parentMap;
	private final HashMap<Integer, HashSet<Integer>> childMap;
	private final HashMap<Integer, Integer> depth;
	private final HashMap<Integer, Integer> leavesMap;

	private int leaves;
	private int size;

	public Hierarchy()
	{
		this.parentMap = new HashMap<>();
		this.childMap = new HashMap<>();
		this.depth = new HashMap<>();
		this.leavesMap = new HashMap<>();
	}

	/**
	 * Locks hierarchy. add() should not be invoked after locking this object
	 *
	 * @param complete if complete, depths are also calculated
	 */
	public void lock()
	{
		this.size = this.parentMap.size() + 1;  //+1 goes for root, which is not put into the map
		this.leaves = this.size - this.childMap.size();

		Integer temp = this.size - 1;
		Integer root = temp;
		do
		{
			temp = this.parentMap.get(temp);
			if (temp != null)
			{
				root = temp;
			}
		} while (temp != null);
		calcLeaves(root);
		calcDepth(root, 0);
	}

	public Integer depth(Integer node)
	{
		return this.depth.get(node);
	}

	private int calcLeaves(Integer node)
	{
		int sum = 0;
		for (Integer child : children(node))
		{
			sum += calcLeaves(child);
		}
		this.leavesMap.put(node, sum);
		return sum == 0 ? 1 : sum;
	}

	private void calcDepth(Integer node, Integer depth)
	{
		this.depth.put(node, depth);
		for (Integer child : this.children(node))
		{
			calcDepth(child, depth + 1);
		}
	}

	public Set<Integer> leaves()
	{
		Set<Integer> nodes = new HashSet<>(this.parentMap.keySet());
		Set<Integer> upperNodes = new HashSet<>(this.childMap.keySet());
		nodes.removeAll(upperNodes);
		return nodes;
	}

	public int hierarchyLeaves()
	{
		return this.leaves;
	}

	public int numOfLeaves(Integer node)
	{
		return this.leavesMap.get(node);
	}

	public int size()
	{
		return this.size;
	}

	public void add(Integer node, Integer parent)
	{
		this.parentMap.put(node, parent);
		HashSet<Integer> cm = this.childMap.get(parent);
		if (cm == null)
		{
			cm = new HashSet<>();
			this.childMap.put(parent, cm);
		}
		cm.add(node);
	}

	public Rule cut(Integer node)
	{
		ArrayList<Integer> list = new ArrayList<>();
		crawlDown(node, list, false);
		if (list.isEmpty())
		{
			return null;
		}
		return new Rule(list, node);
	}

	//returns items below this node
	public void crawlDown(Integer node, ArrayList<Integer> list, boolean leavesOnly)
	{
		for (Integer child : this.children(node))
		{
			if (leavesOnly)
			{
				if (this.childCounter(child).equals(0))
				{
					list.add(child);
				}
			} else
			{
				list.add(child);
			}
			crawlDown(child, list, leavesOnly);
		}
	}

	//returns the children of a node
	public HashSet<Integer> children(Integer node)
	{
		HashSet<Integer> set = this.childMap.get(node);
		if (set == null)
		{
			return new HashSet<>();
		} else
		{
			return set;
		}
	}

	public ArrayList<Integer> leaves(Integer node)
	{
		ArrayList<Integer> list = new ArrayList<>();
		crawlDown(node, list, true);
		return list;
	}

	public Integer parent(Integer node)
	{
		return this.parentMap.get(node);
	}

	public Set<Integer> upperNodes()
	{
		return this.childMap.keySet();
	}

	public Integer childCounter(Integer index)
	{
		if (this.childMap.get(index) == null)
		{
			return 0;
		}
		return this.childMap.get(index).size();
	}

	public Integer lca(Integer a, Integer b)
	{
		int d1 = depth(a);
		int d2 = depth(b);
		while (d1 != d2)
		{
			if (d1 > d2)
			{
				a = this.parentMap.get(a);
				d1--;
			} else
			{
				b = this.parentMap.get(b);
				d2--;
			}
		}
		while (!a.equals(b))
		{
			a = this.parentMap.get(a);
			b = this.parentMap.get(b);
		}
		return a;
	}

	public HashSet<Integer> crawlUp(Integer node)
	{
		HashSet<Integer> set = new HashSet<>();
		Integer parent = this.parent(node);
		while (parent != null)
		{
			set.add(parent);
			parent = this.parent(parent);
		}
		return set;
	}

	//two items are related if they cannot be put in the same set.
	public boolean related(Integer s1, Integer s2)
	{
		Integer d1 = this.depth(s1);
		Integer d2 = this.depth(s2);
		Integer a = s1;
		Integer b = s2;
		if (d1 < d2)
		{
			Integer temp = d2;
			d2 = d1;
			d1 = temp;
			a = s2;
			b = s1;
		}

		do
		{
			if (d1.equals(d2))
			{
				return a.equals(b);
			}
			a = this.parent(a);
			d1 = d1 - 1;

		} while (d2 <= d1);
		return false;
	}

}
