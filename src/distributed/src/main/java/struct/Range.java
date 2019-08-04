package struct;

import java.io.Serializable;

/**
 *
 * @author achilles
 */
public class Range implements Serializable
{

	private double lower;
	private double upper;
	private boolean notSet;

	public Range()
	{
		this.notSet = true;
	}

	public void extend(double x)
	{
		if (this.notSet)
		{
			this.lower = x;
			this.upper = x;
			this.notSet = false;
		} else
		{
			if (x < this.lower)
			{
				this.lower = x;
			}
			if (x > this.upper)
			{
				this.upper = x;
			}
		}
	}

	public double range()
	{
		return this.upper - this.lower;
	}
	
	@Override
	public String toString()
	{
		return this.lower+"-"+this.upper;
	}

}
