package algorithms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import struct.Details;
import struct.Group;
import struct.ULmethod;

/**
 *
 * @author achilles
 */
public class TransactionalAnon
{

	private final Broadcast<Details> meta;

	public TransactionalAnon(Broadcast<Details> meta)
	{
		this.meta = meta;
	}

	public JavaRDD<Group> anon(JavaRDD<Group> group) throws Exception
	{
		if (this.meta.value().getUlFunction().equals(ULmethod.pcta))
		{
			PCTA transf = new PCTA(meta);
			return group.map(transf);
		}

		if (this.meta.value().getUlFunction().equals(ULmethod.hierarchy))
		{
			Apriori transf = new Apriori(meta);
			return group.map(transf);
		}
		throw new Exception("ul function is not set.");
	}

}
