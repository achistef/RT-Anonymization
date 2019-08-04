package structures;

import java.util.ArrayList;
import java.util.BitSet;

/**
 *
 * @author achilles
 */
public class CountTree
{

	private final CountTable table;

	public CountTree()
	{
		this.table = new CountTable();
	}

	public CountTable getTable()
	{
		return this.table;
	}

	public void add(ArrayList<Integer> list)
	{
		ArrayList<Integer> dummy = new ArrayList<>(list);
		this.table.add(dummy);
	}

	public void add(BitSet bitset)
	{
		ArrayList<Integer> dummy = new ArrayList<>();
		int bitCounter = -1;
		while ((bitCounter = bitset.nextSetBit(bitCounter + 1)) != -1)
		{
			dummy.add(bitCounter);
		}
		this.table.add(dummy);
	}

	public int support(ArrayList<Integer> list)
	{
		ArrayList<Integer> dummy = new ArrayList<>(list);
		return this.table.support(dummy);
	}

	public int support(BitSet bitset)
	{
		ArrayList<Integer> list = new ArrayList<>();
		int bitCounter = -1;
		while ((bitCounter = bitset.nextSetBit(bitCounter + 1)) != -1)
		{
			list.add(bitCounter);
		}
		return support(list);
	}

	public boolean isEmpty()
	{
		return this.table.getTable().isEmpty();
	}

}
