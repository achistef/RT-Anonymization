package struct;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 * @author achilles
 * @param <K>
 * @param <V>
 */
public class MyTreeMap<K extends Comparable, V> implements Serializable
{

	private final TreeMap<K, ArrayList<V>> map;
	private final int capacity;
	private int currentSize;

	public MyTreeMap(int size)
	{
		this.capacity = size;
		this.currentSize = 0;
		this.map = new TreeMap<>();

	}

	public void put(K key, V value)
	{
		if (this.currentSize < this.capacity)
		{
			if (!this.map.containsKey(key))
			{
				ArrayList<V> v = new ArrayList<>();
				this.map.put(key, v);
			}
			this.map.get(key).add(value);
			this.currentSize++;
		} else if (this.map.lastKey().compareTo(key) > 0)
		{
			this.pollLastElement();
			this.put(key, value);
		}
	}

	public Entry<K, V> pollFirstElement()
	{
		if (this.map.isEmpty())
		{
			return null;
		}
		K key = this.map.firstKey();
		List<V> list = this.map.get(key);
		V value = list.remove(0);
		if (list.isEmpty())
		{
			this.map.pollFirstEntry();
		}
		this.currentSize--;
		return new AbstractMap.SimpleEntry<>(key, value);
	}

	public Entry<K, V> pollLastElement()
	{
		if (this.map.isEmpty())
		{
			return null;
		}
		K key = this.map.lastKey();
		List<V> list = this.map.get(key);
		V value = list.remove(0);
		if (list.isEmpty())
		{
			this.map.pollLastEntry();
		}
		this.currentSize--;
		return new AbstractMap.SimpleEntry<>(key, value);
	}

	public Entry<K, ArrayList<V>> pollFirstEntry()
	{
		if (this.map.isEmpty())
		{
			return null;
		}
		Entry<K, ArrayList<V>> entry = this.map.pollFirstEntry();
		this.currentSize -= entry.getValue().size();
		return entry;
	}

	public List<V> getValuesDesc()
	{
		List<V> result = new ArrayList<>();
		Iterator<ArrayList<V>> it = this.map.values().iterator();
		while (it.hasNext())
		{
			ArrayList<V> list = it.next();
			result.addAll(list);
		}
		Collections.sort(result, Collections.reverseOrder());
		return result;
	}

	public int size()
	{
		return this.map.size();
	}

	public void removeByValue(V value)
	{
		Iterator<Entry<K, ArrayList<V>>> it = this.map.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<K, ArrayList<V>> entry = it.next();
			ArrayList<V> list = entry.getValue();
			if (list.remove(value))
			{
				this.currentSize--;
				if (list.isEmpty())
				{
					it.remove();
				}
				return;
			}
		}
	}

	public void fixIndexes(int min, int max)
	{
		Iterator<Entry<K, ArrayList<V>>> it = map.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<K, ArrayList<V>> e = it.next();
			ArrayList<Integer> list = (ArrayList<Integer>) e.getValue();
			for (int i = 0; i < list.size(); i++)
			{
				int v = list.get(i);
				if (v > min)
				{
					if (v > max)
					{
						list.set(i, v - 2);
					} else
					{
						list.set(i, v - 1);
					}
				}
			}
		}

	}

	public Set<Entry<K, ArrayList<V>>> entrySet()
	{
		return this.map.entrySet();
	}

	public Collection<ArrayList<V>> values()
	{
		return this.map.values();
	}

	@Override
	public String toString()
	{
		return this.map.toString();
	}

	public int getCapacity()
	{
		return this.capacity;
	}

	public boolean isEmpty()
	{
		return this.currentSize == 0;
	}

}
