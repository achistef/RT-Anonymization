package metric;

import java.io.Serializable;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.broadcast.Broadcast;
import struct.Details;
import struct.Group;
import struct.Hierarchy;
import struct.Instance;

/**
 *
 * @author achilles
 */
public class NcpCalculator implements DoubleFunction<Group>, Serializable
{

	private final Broadcast<Details> meta;
	private final JavaRDD<Group> dataset;

	public NcpCalculator(Broadcast<Details> details, JavaRDD<Group> dataset)
	{
		this.meta = details;
		this.dataset = dataset;
	}

	public Double ncp()
	{
		JavaDoubleRDD mapToDouble = dataset.mapToDouble(this);
		return mapToDouble.sum();
	}
	
	public Double ncpReadable()
	{
		return ncp() / meta.value().getNcpDiv();
	}

	private double ncp(Instance e)
	{
		double result = 0;
		for (int i = 0; i < e.catSize(); i++)
		{
			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(i);
			result += hierarchy.numOfLeaves(e.getCat(i));
		}

		for (int i = 0; i < e.numSize(); i++)
		{
			double range = e.getNum(i).range();
			if (range > 0)
			{
				result += range / this.meta.value().getRanges().get(i).range();
			}
		}
		return result;
	}
//	private double ncp(Instance e)
//	{
//		double result = 0;
//		for (int i = 0; i < e.catSize(); i++)
//		{
//			Hierarchy hierarchy = this.meta.value().getCategoricalHierarchies().get(i);
//			Integer temp = hierarchy.numOfLeaves(e.getCat(i));
//			if (temp > 1)
//			{
//				result += (double)temp / hierarchy.hierarchyLeaves();
//			}
//		}
//
//		for (int i = 0; i < e.numSize(); i++)
//		{
//			double range = e.getNum(i).range();
//			if (range > 0)
//			{
//				result += range / this.meta.value().getRanges().get(i).range();
//			}
//		}
//		return result;
//	}

	@Override
	public double call(Group group) throws Exception
	{
		Instance instance = group.getInstance(0);
		double ncp = ncp(instance);
		return ncp * group.size();
	}
}
