package algorithms;

import java.math.BigDecimal;
import merge.TMerge;
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
public class Tum_Bound
{

	private final Cluster cluster;
	private final TMerge merge;
	private final Dataset dataset;
	private BigDecimal d;

	public Tum_Bound(Dataset dataset)
	{
		this.dataset = dataset;
		this.cluster = new Cluster();
		this.merge = new TMerge();
	}

	public void set_d(double value)
	{
		this.d = new BigDecimal(value);
	}

	public boolean run() throws Exception
	{
		System.out.println("Executing Cluster");
		this.cluster.anonymize(this.dataset);
/*
		if (!(this.dataset.ul().compareTo(this.d) < 1))
		{
			System.out.println("Executing Merge");
			this.merge.RT(this.dataset, this.d);
		}

		System.out.println("Executing transactional anonymization");
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
*/
		//return this.dataset.ul().compareTo(this.d) != 1;
		return false;
	}

}
