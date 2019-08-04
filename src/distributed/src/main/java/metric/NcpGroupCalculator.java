package metric;

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import struct.Details;
import struct.Group;
import struct.Hierarchy;
import struct.Instance;

/**
 *
 * @author achilles
 */
public class NcpGroupCalculator implements Serializable, Function<Group, Tuple2<Double, Group>>
{

	private final Broadcast<Details> meta;

	public NcpGroupCalculator(Broadcast<Details> details)
	{
		this.meta = details;
	}

	private Double ncp(Group group)
	{
		Instance instance = group.getInstance(0);
		double ncp = ncp(instance);
		return ncp * group.size();
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
	public Tuple2<Double, Group> call(Group t)
	{
		return new Tuple2<>(ncp(t), t);
	}
}
