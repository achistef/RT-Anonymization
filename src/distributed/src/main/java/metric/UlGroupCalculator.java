package metric;

import algorithms.Apriori;
import algorithms.PCTA;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;
import struct.Details;
import struct.Group;
import struct.Instance;
import struct.ULmethod;

/**
 *
 * @author achilles
 */
public class UlGroupCalculator implements Serializable, Function<Tuple2<Double, Group>, Tuple3<Double, BigDecimal, Group>>
{

	private final Broadcast<Details> meta;
	private final BigInteger TWO;

	public UlGroupCalculator(Broadcast<Details> details)
	{
		this.meta = details;
		this.TWO = new BigInteger("2");
	}

	@Override
	public Tuple3<Double, BigDecimal, Group> call(Tuple2<Double, Group> tuple2) throws Exception
	{
		if (this.meta.value().getUlFunction().equals(ULmethod.pcta))
		{
			return new Tuple3<>(tuple2._1, ulPCTA(tuple2._2), tuple2._2);
		}

		if (this.meta.value().getUlFunction().equals(ULmethod.hierarchy))
		{
			return new Tuple3<>(tuple2._1, ulHierarchy(tuple2._2), tuple2._2);
		}
		throw new Exception("ul function is not set.");
	}

	private BigDecimal ulPCTA(Group group)
	{
		PCTA pcta = new PCTA(this.meta);
		Group output = pcta.call(group);
		BigInteger result = BigInteger.ZERO;
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < output.size(); i++)
		{
			Instance instance = output.getInstance(i);
			List<BitSet> list = instance.getTraList();
			for (BitSet itemset : list)
			{
				int cardinality = itemset.cardinality();
				Integer get = map.get(cardinality);
				if (get == null)
				{
					map.put(cardinality, 1);
				} else
				{
					map.put(cardinality, get + 1);
				}
			}
		}
		Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext())
		{
			Map.Entry<Integer, Integer> next = iterator.next();
			int cardinality = next.getKey();
			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
			BigInteger ul = cardinality > 1 ? TWO.pow(cardinality).subtract(BigInteger.ONE).multiply(multiplier) : BigInteger.ZERO;
			result = result.add(ul);
		}
		return new BigDecimal(result);
	}

	private BigDecimal ulHierarchy(Group input)
	{
		Apriori apriori = new Apriori(meta);
		Group output = apriori.call(input);
		BigInteger result = BigInteger.ZERO;
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < output.size(); i++)
		{
			Instance instance = output.getInstance(i);
			int bitCounter = -1;
			BitSet items = instance.getTra();
			while ((bitCounter = items.nextSetBit(bitCounter + 1)) != -1)
			{
				Integer get = map.get(bitCounter);
				if (get == null)
				{
					map.put(bitCounter, 1);
				} else
				{
					map.put(bitCounter, get + 1);
				}
			}
		}

		Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
		while (iterator.hasNext())
		{
			Map.Entry<Integer, Integer> next = iterator.next();
			int node = next.getKey();
			BigInteger multiplier = new BigInteger(String.valueOf(next.getValue()));
			BigInteger ul = this.meta.value().getTransactionHierarchy().numOfLeavesExp(node).multiply(multiplier);
			result = result.add(ul);
		}
		return new BigDecimal(result);
	}
}
