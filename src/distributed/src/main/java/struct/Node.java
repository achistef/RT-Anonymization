package struct;

import java.io.Serializable;

/**
 *
 * @author achilles
 */
public class Node implements Serializable
{

	private Integer counter;
	private CountTable table;

	public Node()
	{
		this.counter = 0;
	}

	@Override
	public String toString()
	{
		return String.valueOf(this.counter);
	}

	public Integer getCounter()
	{
		return this.counter;
	}

	public CountTable getTable()
	{
		if (this.table == null)
		{
			this.table = new CountTable();
		}
		return this.table;
	}

	public void increase()
	{
		this.counter++;
	}

}
