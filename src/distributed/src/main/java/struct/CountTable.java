package struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author achilles
 */
public class CountTable implements Serializable
{

	private final HashMap<Integer, Node> map;

	public CountTable()
	{
		this.map = new HashMap<>();
	}

	public int support(ArrayList<Integer> list)
	{
		Integer first = list.remove(0);
		if (this.map.containsKey(first))
		{
			if (list.isEmpty())
			{
				return this.map.get(first).getCounter();
			} else
			{
				return this.map.get(first).getTable().support(list);
			}
		} else
		{
			return 0;
		}

	}

	public HashMap<Integer, Node> getTable()
	{
		return this.map;
	}

	private Node find(Integer num)
	{
		Node result = this.map.get(num);
		if (result == null)
		{
			result = new Node();
			this.map.put(num, result);
		}
		return result;
	}

	public void add(ArrayList<Integer> list)
	{
		if (list.size() == 1)
		{
			add(list.get(0));
			return;
		}
		Integer selected = list.remove(0);
		find(selected).getTable().add(list);
	}

	private void add(Integer num)
	{
		Node selected = find(num);
		selected.increase();
	}

}
