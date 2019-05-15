package transactionalTransformers;

import structures.CountTable;
import structures.CountTree;
import structures.Group;
import structures.Hierarchy;
import structures.MyTreeMap;
import structures.Node;
import structures.Rule;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 *
 * @author achilles
 */
public class Apriori
{

	private final Hierarchy hier;
	private final int k;
	private final int m;
	private int depth;
	private CountTree countTree;

	public Apriori(Hierarchy hierarchy, int k, int m)
	{
		this.hier = hierarchy;
		this.k = k;
		this.m = m;
	}

	public void km(Group group)
	{
		for (this.depth = 1; this.depth <= this.m; this.depth++)
		{
			this.countTree = new CountTree();
			for (int i = 0; i < group.size(); i++)
			{
				BitSet bitset = extend(group.getInstance(i).getTra());
				ArrayList<BitSet> subsets = subsets(bitset, this.depth);
				for (BitSet set : subsets)
				{
					this.countTree.add(set);
				}
			}
			if (this.countTree.isEmpty())
			{
				return;
			}
			applyRules(group, DA());
		}
	}

	public void applyRules(Group group, List<Rule> rules)
	{
		boolean found;
		for (int i = 0; i < group.size(); i++)
		{
			BitSet bitset = group.getInstance(i).getTra();
			for (int j = 0; j < rules.size(); j++)
			{
				found = false;
				Rule rule = rules.get(j);
				for (Integer req : rule.getReqs())
				{
					if (bitset.get(req))
					{
						bitset.set(req, false);
						found = true;
					}
				}
				if (found)
				{
					bitset.set(rule.getRes());
				}
			}
		}

	}

	private ArrayList<Rule> DA()
	{
		ArrayList<Rule> rules = new ArrayList<>();
		dfs(this.countTree.getTable(), new ArrayList<>(), 0, rules);
		return rules;
	}

	private void dfs(CountTable ct, ArrayList<Integer> path, int support, ArrayList<Rule> rules)
	{
		if (path.size() == this.depth)
		{
			if (support < this.k)
			{
				rules.addAll(expansionRules(path));
			}
			return;
		}
		HashMap<Integer, Node> map = ct.getTable();
		Iterator<Entry<Integer, Node>> it = map.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<Integer, Node> entry = it.next();
			Integer key = entry.getKey();
			Node value = entry.getValue();
			path.add(key);
			if (isReqList(rules, path))
			{
				path.remove(path.size() - 1);
				continue;
			}
			dfs(value.getTable(), path, value.getCounter(), rules);
			path.remove(path.size() - 1);
		}
	}

	private ArrayList<Rule> expansionRules(ArrayList<Integer> input)
	{
		ArrayList<Integer> path = new ArrayList<>(input);
		ArrayList<Rule> rules = new ArrayList<>();
		MyTreeMap<Integer, Integer> map = new MyTreeMap<>(path.size());
		for (Integer node : path)
		{
			map.put(this.hier.depth(node), node);
		}

		boolean invalid = true;
		while (invalid)
		{
			Entry<Integer, Integer> entry = map.pollLastElement();
			Integer deepest = entry.getValue();
			Integer parent = this.hier.parent(deepest);
			if (parent == null)
			{
				if (rules.isEmpty())
				{
					rules.add(this.hier.cut(deepest));
				}
				break;
			}
			map.put(entry.getKey() - 1, parent);
			for (Integer child : this.hier.children(parent))
			{
				path.remove(child);
			}
			path.add(parent);
			Collections.sort(path);
			Rule rule = this.hier.cut(parent);
			Iterator<Rule> it = rules.iterator();
			while (it.hasNext())
			{
				Rule selectedRule = it.next();
				if (this.hier.children(rule.getRes()).contains(selectedRule.getRes()))
				{
					it.remove();
				}
			}
			rules.add(rule);
			if (this.countTree.support(path) >= this.k)
			{
				invalid = false;
			}
		}
		return rules;
	}

	private boolean isReq(ArrayList<Rule> rules, Integer node)
	{
		for (Rule rule : rules)
		{
			if (rule.isReq(node))
			{
				return true;
			}
		}
		return false;
	}

	private boolean isReqList(ArrayList<Rule> rules, ArrayList<Integer> path)
	{
		for (Integer node : path)
		{
			if (isReq(rules, node))
			{
				return true;
			}
		}
		return false;
	}

	private BitSet extend(BitSet input)
	{
		BitSet bitset = (BitSet) input.clone();
		BitSet parents = new BitSet();
		int bitCounter = bitset.nextSetBit(0);
		while (bitCounter != -1)
		{
			for (Integer upperNode : this.hier.crawlUp(bitCounter))
			{
				parents.set(upperNode);
			}
			bitCounter = bitset.nextSetBit(bitCounter + 1);
		}
		bitset.or(parents);

		return bitset;
	}

	public ArrayList<BitSet> subsets(BitSet bitset, int n)
	{
		ArrayList<BitSet> list = new ArrayList<>();
		if (bitset.cardinality() < n)
		{
			return list;
		}
		int[] index = new int[n];
		index[0] = bitset.nextSetBit(0);
		for (int i = 1; i < n; i++)
		{
			index[i] = bitset.nextSetBit(index[i - 1] + 1);
		}
		while (index[0] != -1)
		{
			if (!checkForConflict(index))
			{
				BitSet valid = new BitSet();
				for (int i = 0; i < index.length; i++)
				{
					valid.set(index[i]);
				}
				list.add(valid);
			}

			int c = n - 1;
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

			} while (c > -1 && c < n);
		}
		return list;
	}

	private boolean checkForConflict(int[] indices)
	{
		for (int i = 0; i < indices.length - 1; i++)
		{
			int index1 = indices[i];
			for (int j = i + 1; j < indices.length; j++)
			{
				int index2 = indices[j];
				if (this.hier.related(index1, index2))
				{
					return true;
				}
			}
		}
		return false;
	}

}
