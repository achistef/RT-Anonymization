package struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author achilles
 */
public class Instance implements Serializable
{

	private final List<NumAttribute> num;
	private final List<Integer> cat;
	private final BitSet tra;
	private final List<BitSet> traList;

	public Instance()
	{
		this.num = new ArrayList<>();
		this.cat = new ArrayList<>();
		this.tra = new BitSet();
		this.traList = new ArrayList<>();
	}

	public Instance(Instance clone)
	{
		this();
		this.tra.or(clone.getTra());

		for (int i = 0; i < clone.getTraList().size(); i++)
		{
			BitSet bitset = (BitSet) clone.getTraList().get(i).clone();
			this.traList.add(bitset);
		}

		for (int i = 0; i < clone.numSize(); i++)
		{
			this.num.add(new NumAttribute(clone.getNum(i)));
		}

		for (int i = 0; i < clone.catSize(); i++)
		{
			this.cat.add(clone.getCat(i));
		}
	}

	public boolean relationalEquals(Instance instance)
	{
		for (int i = 0; i < this.num.size(); i++)
		{
			if (!this.num.get(i).equals(instance.getNum(i)))
			{
				return false;
			}
		}

		for (int i = 0; i < this.cat.size(); i++)
		{
			if (!this.cat.get(i).equals(instance.getCat(i)))
			{
				return false;
			}
		}
		return true;
	}

	public List<BitSet> getTraList()
	{
		return this.traList;
	}

	public int numSize()
	{
		return this.num.size();
	}

	public int catSize()
	{
		return this.cat.size();
	}

	public void add(NumAttribute v)
	{
		this.num.add(v);
	}

	public void add(Integer s)
	{
		this.cat.add(s);
	}

	public void add(BitSet b)
	{
		this.tra.or(b);
	}

	public void set(int position, NumAttribute v)
	{
		this.num.set(position, v);
	}

	public void set(int position, Integer s)
	{
		this.cat.set(position, s);
	}

	public NumAttribute getNum(int position)
	{
		return this.num.get(position);
	}

	public Integer getCat(int position)
	{
		return this.cat.get(position);
	}

	public BitSet getTra()
	{
		return this.tra;
	}

	public long relationalHash()
	{
		long hash = 0;
		for (int i = 0; i < this.numSize(); i++)
		{
			hash += Objects.hashCode(this.num.get(i)) * (i + 1);
		}
		for (int i = 0; i < this.catSize(); i++)
		{
			hash += Objects.hashCode(this.cat.get(i)) * (i + 1);
		}
		return hash;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.num.size(); i++)
		{
			sb.append(this.num.get(i).toString()).append(",");
		}
		for (int i = 0; i < this.cat.size(); i++)
		{
			sb.append(this.cat.get(i).toString()).append(",");
		}
		if (!this.tra.isEmpty())
		{
			sb.append(this.tra.toString());
		}
		for (int i = 0; i < this.traList.size(); i++)
		{
			sb.append(this.traList.get(i).toString());
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof Instance)
		{
			Instance instance = (Instance) obj;
			for (int i = 0; i < this.num.size(); i++)
			{
				if (!this.num.get(i).equals(instance.getNum(i)))
				{
					return false;
				}
			}

			for (int i = 0; i < this.cat.size(); i++)
			{
				if (!this.cat.get(i).equals(instance.getCat(i)))
				{
					return false;
				}
			}
			if (!this.tra.equals(instance.getTra()))
			{
				return false;
			}
			return true;
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 53 * hash + Objects.hashCode(this.num);
		hash = 53 * hash + Objects.hashCode(this.cat);
		hash = 53 * hash + Objects.hashCode(this.tra);
		hash = 53 * hash + Objects.hashCode(this.traList);
		return hash;
	}

}
