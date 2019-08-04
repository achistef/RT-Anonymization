package struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

/**
 *
 * @author achilles
 */
public class Rule implements Serializable
{

	private final ArrayList<Integer> req;
	private final Integer res;

	public Rule(ArrayList<Integer> list, Integer num)
	{
		this.req = list;
		this.res = num;
	}

	@Override
	public String toString()
	{
		return this.req.toString() + " -> " + this.res;
	}

	public boolean isReq(Integer node)
	{
		return this.req.contains(node);
	}

	public ArrayList<Integer> getReqs()
	{
		return this.req;
	}

	public Integer getRes()
	{
		return this.res;
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof Rule)
		{
			Rule rule = (Rule) o;
			if (this.res.equals(rule.getRes()))
			{
				if (this.req.equals(rule.getReqs()))
				{
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 59 * hash + Objects.hashCode(this.req);
		hash = 59 * hash + Objects.hashCode(this.res);
		return hash;
	}

}
