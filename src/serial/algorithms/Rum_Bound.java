package algorithms;

import merge.RMerge;
import transactionalTransformers.Apriori;
import relationalTransformers.Cluster;
import structures.Dataset;
import structures.Group;
import structures.ULmethod;
import transactionalTransformers.PCTA;

/**
 *
 * @author achilles
 */
public class Rum_Bound
{

	private final Cluster cluster;
	private final RMerge merge;
	private final Dataset dataset;
	private double d;

	public Rum_Bound(Dataset dataset)
	{
		this.dataset = dataset;
		this.cluster = new Cluster();
		this.merge = new RMerge();
	}

	public void set_d(double value)
	{
		this.d = value;
	}

	public boolean run() throws Exception
	{
		this.cluster.anonymize(this.dataset);
		if (this.dataset.ncp() > this.d)
		{
			return false;
		}
		
//		long t = System.currentTimeMillis();
//		this.merge.T(this.dataset, this.d);
//		System.out.println("merge took " + (System.currentTimeMillis()-t) +" ms");
		
		for (int i = 0; i < this.dataset.numOfGroups(); i++)
		{
			Group group = this.dataset.getGroup(i);
			if (dataset.getUlFunction().equals(ULmethod.pcta))
			{
				PCTA algo = new PCTA(group, dataset.getK(), dataset.getM());
				algo.pcta();
				algo.applyRules();
			}

			if (dataset.getUlFunction().equals(ULmethod.hierarchy))
			{
				Apriori apriori = new Apriori(this.dataset.transactionHierarchy(), dataset.getK(), dataset.getM());
				apriori.km(group);
			}

		}
		return true;
	}

}
