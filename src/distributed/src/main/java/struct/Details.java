package struct;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author achilles
 */
public class Details implements Serializable
{

	private String splitter;
	private List<Character> order;
	private final List<Range> ranges;
	private final List<Hierarchy> categoricalHierarchies;
	private final List<SI_Map> categoricalMaps;
	private final Hierarchy transactionHierarchy;
	private final SI_Map transactionMap;
	private BigDecimal transactionSR;
	private int k;
	private int m;
	private double rd;
	private BigDecimal td;
	private ULmethod method;
	private int numOfPartitions;
	private int maxConflicts;
	private int maxClusters;
	private int maxMerges;
	private int maxMergeTreeSize;
	private int minMergeTreeSize;
	private final int numOfAttributes;
	private int numOfInstances;
	private int ncpDiv;

	public Details(String hPath, String datasetName)
	{
		this.order = new ArrayList<>();
		this.ranges = new ArrayList<>();
		this.categoricalHierarchies = new ArrayList<>();
		this.categoricalMaps = new ArrayList<>();
		this.transactionHierarchy = new Hierarchy();
		this.transactionMap = new SI_Map();
		this.transactionSR = BigDecimal.ZERO;

		JavaSparkContext sc = MySparkContext.getSparkContext();
		String fileHeader = sc.textFile(datasetName).first();
		for (String att : fileHeader.split(","))
		{
			String[] split = att.split("#");
			String attName = split[0];
			char attType = split[1].charAt(0);

			this.order.add(attType);
			List<String> list = sc.textFile(hPath + attName).collect();
			switch (attType)
			{
				case 'c':
				{
					SI_Map map = new SI_Map();
					Hierarchy h = new Hierarchy();
					this.categoricalHierarchies.add(h);

					for (String line : list)
					{
						String[] splitInHalf = line.split(":");
						String parent = splitInHalf[0];
						String[] children = splitInHalf[1].split(",");
						for (String child : children)
						{
							h.add(map.hash(child), map.hash(parent));
						}
					}
					h.lock();
					this.categoricalMaps.add(map);
					break;
				}

				case 'n':
				{
					Range range = new Range();
					String line = list.get(0);
					String[] minmax = line.split("-");
					range.extend(Integer.valueOf(minmax[0]));
					range.extend(Integer.valueOf(minmax[1]));
					this.ranges.add(range);
					break;
				}

				case 't':
				{
					for (String line : list)
					{
						String[] splitInHalf = line.split(":");
						String parent = splitInHalf[0];
						String[] children = splitInHalf[1].split(",");
						for (String child : children)
						{
							this.transactionHierarchy.add(this.transactionMap.hash(child), this.transactionMap.hash(parent));
						}
					}
					this.transactionHierarchy.lock();
					this.transactionSR = new BigDecimal(new BigInteger("2").pow(this.transactionHierarchy.hierarchyLeaves())).subtract(BigDecimal.ONE);
					break;
				}
			}
		}

		this.numOfAttributes = order.size() - 1;

		//default values
		this.splitter = ",";
		this.minMergeTreeSize = 1;
		this.maxMergeTreeSize = 5;
		this.rd = 1;
		this.td = new BigDecimal(BigInteger.ZERO);

		int numOfExecutors;
		try
		{
			numOfExecutors = Integer.parseInt(sc.getConf().get("spark.executor.instances"));
		} catch (Exception e)
		{
			numOfExecutors = 1;
		}
		int numOfCores = Integer.parseInt(sc.getConf().get("spark.executor.cores"));
		int partitions = numOfExecutors * numOfCores;
		this.numOfPartitions = 2*partitions;
		System.out.println("Achilles Number of partitions set to " + partitions);
	}

	public int getMinMergeTreeSize()
	{
		return this.minMergeTreeSize;
	}

	public void setMinMergeTreeSize(int m)
	{
		this.minMergeTreeSize = m;
	}

	public int getMaxMergeTreeSize()
	{
		return this.maxMergeTreeSize;
	}

	public void setMaxMergeTreeSize(int m)
	{
		this.maxMergeTreeSize = m;
	}

	public int getMaxMerges()
	{
		return this.maxMerges;
	}

	public void setMaxMerges(int m)
	{
		this.maxMerges = m;
	}

	public void setNumOfInstances(int num)
	{
		this.numOfInstances = num;
		this.ncpDiv = num * this.numOfAttributes;

		this.rd = this.rd * this.ncpDiv;
		this.td = this.td.multiply(this.transactionSR.multiply(new BigDecimal(this.numOfInstances)));
	}

	public int getNumOfInstances()
	{
		return this.numOfInstances;
	}

	public int getNcpDiv()
	{
		return this.ncpDiv;
	}

	public void setSplitter(String splitter)
	{
		this.splitter = splitter;
	}

	public String getSplitter()
	{
		return splitter;
	}

	public void setMaxConflicts(int conf)
	{
		this.maxConflicts = conf;
	}

	public int getMaxConflicts()
	{
		return maxConflicts;
	}

	public void setMaxClusters(int conf)
	{
		this.maxClusters = conf;
	}

	public int getMaxClusters()
	{
		return maxClusters;
	}

	public void setNumOfPartitions(int num)
	{
		this.numOfPartitions = num;
	}

	public int getNumOfPartitions()
	{
		return this.numOfPartitions;
	}

	public void setRd(double d)
	{
		this.rd = d;
	}

	public double getRd()
	{
		return rd;
	}

	public void setTd(double d)
	{
		this.td = new BigDecimal(d);
	}

	public BigDecimal getTd()
	{
		return td;
	}

	public int getNumOfAttributes()
	{
		return this.numOfAttributes;
	}

	public int getK()
	{
		return k;
	}

	public int getM()
	{
		return m;
	}

	public void setK(int k)
	{
		this.k = k;
		//default value
		setMaxConflicts(k / 2);
	}

	public void setM(int m)
	{
		this.m = m;
	}

	public ULmethod getUlFunction()
	{
		return method;
	}

	public void setUlFunction(ULmethod method)
	{
		this.method = method;
	}

	public List<Character> getOrder()
	{
		return order;
	}

	public List<Range> getRanges()
	{
		return ranges;
	}

	public List<Hierarchy> getCategoricalHierarchies()
	{
		return categoricalHierarchies;
	}

	public List<SI_Map> getCategoricalMaps()
	{
		return categoricalMaps;
	}

	public Hierarchy getTransactionHierarchy()
	{
		return transactionHierarchy;
	}

	public SI_Map getTransactionMap()
	{
		return transactionMap;
	}

	public BigDecimal getTransactionSR()
	{
		return transactionSR;
	}

}
