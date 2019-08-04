package struct;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 *
 * @author achilles
 */
public class SI_Map implements Serializable
{

	private final HashMap<String, Integer> map;
	private int counter;

	public SI_Map()
	{
		this.map = new HashMap<>();
		this.counter = 0;
	}

	public Integer hash(String key)
	{
		if (!map.containsKey(key))
		{
			map.put(key, counter);
			counter++;
		}
		return map.get(key);
	}

	public HashMap<Integer, String> reverse()
	{
		HashMap<Integer, String> result = new HashMap<>();
		Iterator<Entry<String, Integer>> it = this.map.entrySet().iterator();
		while (it.hasNext())
		{
			Entry<String, Integer> entry = it.next();
			result.put(entry.getValue(), entry.getKey());
		}
		return result;
	}

	@Override
	public String toString()
	{
		return String.valueOf(counter);
	}
	
}
