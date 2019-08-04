package struct;

import java.io.Serializable;

/**
 *
 * @author achilles
 */
public class NumAttribute implements Serializable
{

	private double lower;
	private double upper;

	public NumAttribute(double a, double b)
	{
		this.lower = a;
		this.upper = b;
	}

	public NumAttribute(NumAttribute attribute)
	{
		this(attribute.getLower(), attribute.getUpper());
	}

	public double getUpper()
	{
		return this.upper;
	}

	public double getLower()
	{
		return this.lower;
	}

	public double range()
	{
		return this.upper - this.lower;
	}

	public void extend(double number)
	{
		if (number < this.lower)
		{
			this.lower = number;
		}
		if (number > this.upper)
		{
			this.upper = number;
		}
	}

	public void extend(NumAttribute attribute)
	{
		extend(attribute.getLower());
		extend(attribute.getUpper());
	}

	@Override
	public boolean equals(Object o)
	{
		if (o instanceof NumAttribute)
		{
			NumAttribute attribute = (NumAttribute) o;
			if (this.lower == attribute.getLower() && this.upper == attribute.getUpper())
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 43 * hash + (int) (Double.doubleToLongBits(this.lower) ^ (Double.doubleToLongBits(this.lower) >>> 32));
		hash = 43 * hash + (int) (Double.doubleToLongBits(this.upper) ^ (Double.doubleToLongBits(this.upper) >>> 32));
		return hash;
	}

	@Override
	public String toString()
	{
		if (this.lower == this.upper)
		{
			if (Math.floor(this.lower) == this.lower)
			{
				return String.valueOf((int) this.lower);
			}
			return String.valueOf(this.lower);
		}

		String l = String.valueOf(this.lower);
		String u = String.valueOf(this.upper);

		if (Math.floor(this.lower) == this.lower)
		{
			l = String.valueOf((int) this.lower);
		}
		if (Math.floor(this.upper) == this.upper)
		{
			u = String.valueOf((int) this.upper);
		}

		return l + "-" + u;
	}

}
