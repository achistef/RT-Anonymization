package relationalTransformers;

import structures.Dataset;
import structures.Group;
import structures.MyTreeMap;
import structures.Instance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author achilles
 */
public class Cluster
{

	public Cluster()
	{
		//empty constructor.
	}

	public void anonymize(Dataset dataset)
	{
		int k = dataset.getK();
		//create groups of size k
		Group seed = dataset.removeGroup(0);
		while (seed.size() >= k)
		{
			Group group = new Group();
			Instance instance = seed.removeInstance(0);
			group.addInstance(instance);
			MyTreeMap<Double, Integer> map = new MyTreeMap<>(k - 1);

			int zeroCounter = 0;
			for (int i = 0; i < seed.size(); i++)
			{
				Instance temp = seed.getInstance(i);
				double ncp = dataset.ncp(instance, temp);
				map.put(ncp, i);
				if (ncp == 0)
				{
					zeroCounter++;
					if (zeroCounter == k - 1)
					{
						//break;
					}
				}
			}

			List<Integer> list = map.getValuesDesc();
			for (int i = 0; i < list.size(); i++)
			{
				Instance selected = seed.removeInstance(list.get(i));
				group.addInstance(selected);
			}

			dataset.addGroup(group);
		}

		//accomodate the remaining records ( k-1 utmost )
		List<Instance> status = new ArrayList<>();
		int counter = seed.size();
		if (counter > 0)
		{
			for (int i = 0; i < dataset.numOfGroups(); i++)
			{
				Group group = dataset.getGroup(i);
				Instance generalized = dataset.generalize(group);
				status.add(generalized);
			}
		}

		while (seed.size() > 0)
		{
			Instance instance = seed.removeInstance(0);
			MyTreeMap<Double, Integer> map = new MyTreeMap<>(1);
			for (int index = 0; index < status.size(); index++)
			{
				Instance temp = status.get(index);
				double ncp = dataset.ncp(instance, temp) * dataset.getGroup(index).size();
				map.put(ncp, index);
			}

			int position = map.pollFirstElement().getValue();
			Instance generalized = dataset.merge(instance, status.get(position));
			status.set(position, generalized);
			dataset.getGroup(position).addInstance(instance);
		}

		//apply G to the relational attributes
		for (int i = 0; i < dataset.numOfGroups(); i++)
		{
			dataset.anonymize(dataset.getGroup(i));
		}

		//extend clusters
		MyTreeMap<Long, Integer> map = new MyTreeMap<>(dataset.numOfGroups());

		for (int i = 0; i < dataset.numOfGroups(); i++)
		{
			Instance instance = dataset.getGroup(i).getInstance(0);
			map.put(instance.relationalHash(), i);
		}

		List<Integer> toBeRemoved = new ArrayList<>();
		int numOfLists = map.size();
		for (int i = 0; i < numOfLists; i++)
		{
			ArrayList<Integer> list = map.pollFirstEntry().getValue();
			if (list.size() > 1)
			{
				int size = list.size();
				boolean table[] = new boolean[size];

				for (int j = 0; j < size - 1; j++)
				{
					if (table[j])
					{
						continue;
					}
					Group firstGroup = dataset.getGroup(list.get(j));
					for (int l = j + 1; l < size; l++)
					{
						if (table[l])
						{
							continue;
						}
						Group secondGroup = dataset.getGroup(list.get(l));
						if (firstGroup.relationalEquals(secondGroup))
						{
							toBeRemoved.add(list.get(l));
							firstGroup.shallowMerge(secondGroup);
							table[l] = true;
						}
					}

				}
			}
		}
		Collections.sort(toBeRemoved, Collections.reverseOrder());
		for (Integer group : toBeRemoved)
		{
			dataset.removeGroup(group);
		}
	}

}
