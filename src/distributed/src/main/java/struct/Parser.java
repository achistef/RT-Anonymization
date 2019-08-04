package struct;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/**
 *
 * @author achilles
 */
public class Parser implements Function<String, Instance>, Serializable
{

	private final Broadcast<Details> meta;

	public Parser(Broadcast<Details> details)
	{
		this.meta = details;
	}

	@Override
	public Instance call(String line)
	{
		String[] tokens = line.split(this.meta.value().getSplitter());
		Instance instance = new Instance();
		int categoricalCounter = 0;
		for (int i = 0; i < tokens.length; i++)
		{
			String token = tokens[i];
			char attributeType = this.meta.value().getOrder().get(i);
			switch (attributeType)
			{
				case 'n':
				{
					double number = Double.parseDouble(token);
					instance.add(new NumAttribute(number, number));
					break;
				}
				case 'c':
				{
					instance.add(this.meta.value().getCategoricalMaps().get(categoricalCounter).hash(token));
					categoricalCounter++;
					break;
				}
				case 't':
				{
					for (String item : token.trim().split(" "))
					{
						instance.getTra().set(this.meta.value().getTransactionMap().hash(item));
					}
					break;
				}
			}
		}
		return instance;
	}

}
