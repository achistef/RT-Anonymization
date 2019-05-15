package algorithms;

import java.util.ArrayList;
import java.util.List;
import structures.Dataset;
import structures.Group;
import structures.Instance;
import transactionalTransformers.PCTA;

/**
 *
 * @author achilles
 */
public class test
{

	public static void main(String[] args)
	{
		Dataset dataset = new Dataset();
		List<Character> order = new ArrayList<>();
		order.add('t');
		dataset.setOrder(order);
		Group group = new Group();
		dataset.addGroup(group);

		Instance e = new Instance();
		e.getTra().set(1);
		e.getTra().set(2);
		group.addInstance(e);

		PCTA p = new PCTA(dataset.getGroup(0), 3, 2);
		p.pcta();
	}

}
