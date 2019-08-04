package structures;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author achilles
 */
public class Group
{

	private final List<Instance> list;
	private boolean generalized;

	public Group()
	{
		this.list = new ArrayList<>();
		this.generalized = false;
	}

	public Group(Group clone)
	{
		this();
		this.generalized = clone.isGeneralized();
		for (int i = 0; i < clone.size(); i++)
		{
			this.list.add(new Instance(clone.getInstance(i)));
		}
	}

	public boolean isGeneralized()
	{
		return this.generalized;
	}

	public void setGeneralized(boolean f)
	{
		this.generalized = f;
	}

	public void addInstance(Instance instance)
	{
		this.list.add(instance);
	}

	public Instance getInstance(int index)
	{
		return this.list.get(index);
	}

	public Instance removeInstance(int index)
	{
		return this.list.remove(index);
	}

	//shallow copy.
	public void shallowMerge(Group group)
	{
		for (int i = 0; i < group.size(); i++)
		{
			this.list.add(group.getInstance(i));
		}
	}

	public int size()
	{
		return this.list.size();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.list.size(); i++)
		{
			sb.append("\n").append(this.list.get(i).toString());
		}
		return sb.toString();
	}

	public boolean relationalEquals(Group group)
	{
		if (this.generalized == group.isGeneralized() == true)
		{
			if (this.list.get(0).relationalEquals(group.getInstance(0)))
			{
				return true;
			}
		} else if (this.generalized == group.isGeneralized() == false)
		{
			for (int i = 0; i < this.list.size(); i++)
			{
				if (!this.list.get(i).relationalEquals(group.getInstance(i)))
				{
					return false;
				}
			}
			return true;
		}
		return false;
	}

}
