package merge;

import structures.Dataset;
import structures.Group;
import structures.MyTreeMap;
import structures.Instance;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 *
 * @author achilles
 */
public class RMerge
{

	public RMerge()
	{
		//empty constructor
	}

	public void R(Dataset dataset, double d)
	{
		MyTreeMap<Double, Integer> ncpMap = NCP_map(dataset);
		double totalNcp = dataset.ncp();
		int datasetSize = dataset.numOfInstances();

		boolean changes;
		do
		{
			changes = false;
			if (dataset.numOfGroups() < 2)
			{
				return;
			}

			Entry<Double, Integer> entry = ncpMap.pollFirstElement();
			int indexC = entry.getValue();
			Group C = dataset.getGroup(indexC);
			double rumC = entry.getKey();
			Instance instanceC = C.getInstance(0);

			int indexC2 = 0;
			double temp = Double.POSITIVE_INFINITY;

			for (int i = 0; i < dataset.numOfGroups(); i++)
			{
				if (i != indexC)
				{
					Group C2 = dataset.getGroup(i);
					Instance instanceC2 = C2.getInstance(0);
					double rumC_C2 = dataset.ncp(instanceC, instanceC2) * (C.size() + C2.size()) / datasetSize;

					if (rumC_C2 < temp)
					{
						temp = rumC_C2;
						indexC2 = i;
					}
				}
			}

			Group C2 = dataset.getGroup(indexC2);
			double rumC2 = dataset.ncp(C2);
			Instance instanceC2 = C2.getInstance(0);

			double difference = temp - rumC - rumC2;
			if (totalNcp + difference <= d)
			{
				totalNcp = totalNcp + difference;
//				System.out.println(totalNcp);

				Instance mergedInstance = dataset.merge(instanceC, instanceC2);
				Group group = mergeGroups(C, C2, mergedInstance);
				dataset.addGroup(group);

				int max = Math.max(indexC, indexC2);
				int min = Math.min(indexC, indexC2);

				dataset.removeGroup(max);
				dataset.removeGroup(min);
				ncpMap.removeByValue(indexC2);
				ncpMap.fixIndexes(min, max);

				ncpMap.put(dataset.ncp(group), dataset.numOfGroups() - 1);
				changes = true;
			}
		} while (changes);
	}

	public void T(Dataset dataset, double d)
	{

		MyTreeMap<Double, Integer> ncpMap = NCP_map(dataset);
		double totalNcp = dataset.ncp();
		int datasetSize = dataset.numOfInstances();

		boolean changes;
		do
		{
			changes = false;
			if (dataset.numOfGroups() < 2)
			{
				return;
			}

			Entry<Double, Integer> entry = ncpMap.pollFirstElement();
			int indexC = entry.getValue();
			Group C = dataset.getGroup(indexC);
			double rumC = entry.getKey();
			int sizeC = C.size();
			MyTreeMap<Double, Integer> btdMap = BTD_map(dataset, indexC);

			do
			{
				entry = btdMap.pollFirstElement();
				if (entry == null)
				{
					return;
				}
				int indexC2 = entry.getValue();
				Group C2 = dataset.getGroup(indexC2);
				double rumC2 = dataset.ncp(C2);
				int sizeC2 = C2.size();

				double mergeNcp = dataset.ncp(C.getInstance(0), C2.getInstance(0));
				double groupNcp = ((sizeC + sizeC2) * mergeNcp) / datasetSize;
				double difference = groupNcp - rumC - rumC2;
				if (totalNcp + difference <= d)
				{
					totalNcp = totalNcp + difference;
					System.out.println(totalNcp);
					Instance merge = dataset.merge(C.getInstance(0), C2.getInstance(0));
					Group group = mergeGroups(C, C2, merge);
					dataset.addGroup(group);

					int max = Math.max(indexC, indexC2);
					int min = Math.min(indexC, indexC2);

					dataset.removeGroup(max);
					dataset.removeGroup(min);
					ncpMap.removeByValue(indexC2);
					ncpMap.fixIndexes(min, max);

					ncpMap.put(dataset.ncp(group), dataset.numOfGroups() - 1);
					changes = true;
				}
			} while (!changes);
		} while (changes);
	}

	public void RT(Dataset dataset, double d)
	{
		MyTreeMap<Double, Integer> ncpMap = NCP_map(dataset);
		double totalNcp = dataset.ncp();
		int datasetSize = dataset.numOfInstances();
		long t = System.currentTimeMillis();
		boolean changes;
		do
		{
			changes = false;
			if (dataset.numOfGroups() < 2)
			{
				return;
			}

			Entry<Double, Integer> entry = ncpMap.pollFirstElement();
			double rumC = entry.getKey();
			int indexC = entry.getValue();
			Group C = dataset.getGroup(indexC);
			int sizeC = C.size();

			MyTreeMap<Double, Integer> rumMap = NCP_map(dataset, indexC);
			MyTreeMap<Double, Integer> tumMap = BTD_map(dataset, indexC);

			List<Integer> indices = new ArrayList<>();
			for (int i = 0; i < dataset.numOfGroups(); i++)
			{
				indices.add(0);
			}
			count(indices, rumMap);
			count(indices, tumMap);

			MyTreeMap<Integer, Integer> sortMap = new MyTreeMap<>(dataset.numOfGroups() - 1);
			for (int i = 0; i < indices.size(); i++)
			{
				if (i != indexC)
				{
					sortMap.put(indices.get(i), i);
				}
			}

			do
			{
				Entry<Integer, Integer> tempEntry = sortMap.pollFirstElement();
				if (tempEntry == null)
				{
					return;
				}
				int indexC2 = tempEntry.getValue();
				Group C2 = dataset.getGroup(indexC2);
				int sizeC2 = C2.size();
				double rumC2 = dataset.ncp(C2);
				double mergeNcp = dataset.ncp(C.getInstance(0), C2.getInstance(0));
				double rumC_C2 = ((sizeC + sizeC2) * mergeNcp) / datasetSize;
				double difference = rumC_C2 - rumC - rumC2;
				if (totalNcp + difference <= d)
				{
					totalNcp = totalNcp + difference;
					System.out.println(totalNcp);
					Instance mergeInstance = dataset.merge(C.getInstance(0), C2.getInstance(0));

					Group group = mergeGroups(C, C2, mergeInstance);

					int max = Math.max(indexC, indexC2);
					int min = Math.min(indexC, indexC2);

					dataset.addGroup(group);

					dataset.removeGroup(max);
					dataset.removeGroup(min);

					ncpMap.removeByValue(indexC2);
					ncpMap.fixIndexes(min, max);
					ncpMap.put(dataset.ncp(group), dataset.numOfGroups() - 1);

					changes = true;
				}
			} while (!changes);
		} while (changes);
		System.out.println("merge took"+(System.currentTimeMillis() - t));
	}

	private void count(List<Integer> indices, MyTreeMap<Double, Integer> map)
	{
		int counter = map.size();
		for (int i = 0; i < counter; i++)
		{
			List<Integer> list = map.pollFirstEntry().getValue();
			for (int j = 0; j < list.size(); j++)
			{
				int t = list.get(j);
				indices.set(t, indices.get(t) + i + 1);
			}
		}

	}

	private Group mergeGroups(Group a, Group b, Instance mergedInstance)
	{
		Group group = new Group();
		group.setGeneralized(true);

		addMerged(group, a, mergedInstance);
		addMerged(group, b, mergedInstance);
		return group;
	}

	private void addMerged(Group group, Group temp, Instance instance)
	{
		for (int i = 0; i < temp.size(); i++)
		{
			Instance clone = new Instance(instance);
			clone.getTra().clear();
			clone.add(temp.getInstance(i).getTra());
			group.addInstance(clone);
		}
	}

	private MyTreeMap<Double, Integer> BTD_map(Dataset dataset, int exclude)
	{
		MyTreeMap<Double, Integer> map = new MyTreeMap<>(dataset.numOfGroups());
		Group selected = dataset.getGroup(exclude);
		for (int i = 0; i < dataset.numOfGroups(); i++)
		{
			if (i != exclude)
			{
				map.put(dataset.btd(selected, dataset.getGroup(i)), i);
			}
		}
		return map;
	}

	private MyTreeMap<Double, Integer> NCP_map(Dataset dataset)
	{
		MyTreeMap<Double, Integer> map = new MyTreeMap<>(dataset.numOfGroups());
		for (int i = 0; i < dataset.numOfGroups(); i++)
		{
			Group group = dataset.getGroup(i);
			map.put(dataset.ncp(group), i);
		}
		return map;
	}

	private MyTreeMap<Double, Integer> NCP_map(Dataset dataset, int exclude)
	{
		int datasetSize = dataset.numOfInstances();
		Group selected = dataset.getGroup(exclude);
		Instance instance = selected.getInstance(0);
		int selectedSize = selected.size();
		MyTreeMap<Double, Integer> map = new MyTreeMap<>(dataset.numOfGroups());
		for (int i = 0; i < dataset.numOfGroups(); i++)
		{
			if (i != exclude)
			{
				Group group = dataset.getGroup(i);
				Instance temp = group.getInstance(0);
				int size = group.size();
				double rum = dataset.ncp(instance, temp) * (selectedSize + size) / datasetSize;
				map.put(rum, i);
			}
		}
		return map;
	}

}
