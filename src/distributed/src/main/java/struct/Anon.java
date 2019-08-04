/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package struct;

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 *
 * @author achilles
 */
public class Anon implements Function<Tuple2<Long, Group>, Group>, Serializable
{

	private final Broadcast<Details> meta;

	public Anon(Broadcast<Details> details)
	{
		this.meta = details;
	}

	@Override
	public Group call(Tuple2<Long, Group> t1) throws Exception
	{
		Group g = new Group(t1._2);
		anonymize(g);
		return g;
	}

	private Instance generalize(Group group)
	{
		Instance instance = new Instance(group.getInstance(0));

		for (int i = 1; i < group.size(); i++)
		{
			Instance temp = group.getInstance(i);
			for (int j = 0; j < instance.numSize(); j++)
			{
				instance.getNum(j).extend(temp.getNum(j));
			}

			for (int j = 0; j < instance.catSize(); j++)
			{
				int lca = this.meta.value().getCategoricalHierarchies().get(j).lca(instance.getCat(j), temp.getCat(j));
				instance.set(j, lca);
			}
		}
		return instance;
	}

	private void anonymize(Group group)
	{
		Instance instance = generalize(group);

		for (int i = 0; i < group.size(); i++)
		{
			Instance clone = new Instance(instance);
			clone.getTra().clear();
			clone.add(group.removeInstance(0).getTra());
			group.addInstance(clone);
		}
		group.setGeneralized(true);
	}

}
