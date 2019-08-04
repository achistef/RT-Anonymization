package struct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 *
 * @author achilles
 */
public class ParserReverse implements Function<Group, String>, Serializable
{

	private final Broadcast<Details> meta;

	public ParserReverse(Broadcast<Details> details)
	{
		this.meta = details;
	}

	@Override
	public String call(Group input)
	{
		StringBuilder sb = new StringBuilder();
		HashMap<Integer, String> transactionMapReverse = this.meta.value().getTransactionMap().reverse();
		ArrayList<HashMap<Integer, String>> categoricalMapsReverse = new ArrayList<>();
		this.meta.value().getCategoricalMaps().forEach((si)
			-> 
			{
				categoricalMapsReverse.add(si.reverse());
		});

		for (int i = 0; i < input.size(); i++)
		{
			Instance instance = input.getInstance(i);
			int categoricalCounter = 0;
			int numericCounter = 0;
			for (int l = 0; l < this.meta.value().getOrder().size(); l++)
			{
				char attributeType = this.meta.value().getOrder().get(l);
				switch (attributeType)
				{
					case 'n':
					{
						sb.append(instance.getNum(numericCounter));
						numericCounter++;
						break;
					}
					case 'c':
					{
						sb.append(categoricalMapsReverse.get(categoricalCounter).get(instance.getCat(categoricalCounter)));
						categoricalCounter++;
						break;
					}
					case 't':
					{
						BitSet bitset = instance.getTra();
						if (bitset.cardinality() > 0)
						{
							int bitCounter = bitset.nextSetBit(0);
							while (bitCounter != -1)
							{
								sb.append(transactionMapReverse.get(bitCounter));
								bitCounter = bitset.nextSetBit(bitCounter + 1);
								if (bitCounter != -1)
								{
									sb.append(" ");
								}
							}
						}
						List<BitSet> list = instance.getTraList();
						if (!list.isEmpty())
						{
							for (BitSet itemset : list)
							{
								sb.append("{");
								int bitCounter = itemset.nextSetBit(0);
								while (bitCounter != -1)
								{
									sb.append(transactionMapReverse.get(bitCounter));
									bitCounter = itemset.nextSetBit(bitCounter + 1);
									if (bitCounter != -1)
									{
										sb.append(" ");
									}
								}
								sb.append("}");
							}
						}
						break;
					}
				}
				if (l != this.meta.value().getOrder().size() - 1)
				{
					sb.append(",");
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}

}
