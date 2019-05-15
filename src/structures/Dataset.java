package structures;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import transactionalTransformers.Apriori;
import transactionalTransformers.PCTA;

/**
 *
 * @author achilles
 */
public class Dataset
{

	private static final BigInteger TWO = new BigInteger("2");
	private String splitter;
	private final List<Group> dataset;
	private List<Character> order;
	private final List<SI_Map> categoricalMaps;
	private List<Hierarchy> categoricalHierarchies;
	private final List<Range> ranges;
	private List<Double> importance;
	private final SI_Map transactionMap;
	private Hierarchy transactionHierarchy;
	private BigDecimal transactionSR;
	private int k;
	private int m;
	private ULmethod method;

	public Dataset()
	{
		this.dataset = new ArrayList<>();
		this.categoricalMaps = new ArrayList<>();
		this.ranges = new ArrayList<>();
		this.transactionMap = new SI_Map();
	}

	public void saveAs(String name) throws IOException
	{
		ArrayList<HashMap<Integer, String>> categoricalMapsReverse = new ArrayList<>();
		for (SI_Map si : this.categoricalMaps)
		{
			categoricalMapsReverse.add(si.reverse());
		}
		HashMap<Integer, String> transactionMapReverse = this.transactionMap.reverse();
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(name), "utf-8")))
		{
			StringBuilder sb = new StringBuilder();
			for (Group g : this.dataset)
			{
				for (int i = 0; i < g.size(); i++)
				{
					Instance instance = g.getInstance(i);
					int categoricalCounter = 0;
					int numericCounter = 0;
					for (int l = 0; l < this.order.size(); l++)
					{
						char attributeType = order.get(l);
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
						if (l != this.order.size() - 1)
						{
							sb.append(",");
						}
					}
					sb.append("\n");
				}
			}
			writer.write(sb.toString());
			writer.close();
		}
	}

	public void read(String filename)
	{
		Group g = new Group();
		this.dataset.add(g);
		String line;
		try (BufferedReader reader = new BufferedReader(new FileReader(filename)))
		{
			while ((line = reader.readLine()) != null)
			{
				String[] tokens = line.split(this.splitter);
				Instance instance = new Instance();
				int categoricalCounter = 0;
				int numericCounter = 0;
				for (int i = 0; i < tokens.length; i++)
				{
					String token = tokens[i];
					char attributeType = order.get(i);
					switch (attributeType)
					{
						case 'n':
						{
							double number = Double.parseDouble(token);
							instance.add(new NumAttribute(number, number));
							this.ranges.get(numericCounter).extend(number);
							numericCounter++;
							break;
						}
						case 'c':
						{
							instance.add(this.categoricalMaps.get(categoricalCounter).hash(token));
							categoricalCounter++;
							break;
						}
						case 't':
						{
							for (String item : token.trim().split(" "))
							{
								instance.getTra().set(this.transactionMap.hash(item));
							}
							break;
						}
					}
				}
				g.addInstance(instance);
			}
			reader.close();
		} catch (IOException exception)
		{
			System.out.println("Something went wrong with " + filename);
		}

		this.categoricalHierarchies = new ArrayList<>();
		for (int i = 0; i < this.categoricalMaps.size(); i++)
		{
			SI_Map map = this.categoricalMaps.get(i);
			Hierarchy h = new Hierarchy();
			this.categoricalHierarchies.add(h);
			String Hfilename = "hierarchies/h" + i;
			try (BufferedReader reader = new BufferedReader(new FileReader(Hfilename)))
			{
				while ((line = reader.readLine()) != null)
				{
					String[] splitInHalf = line.split(":");
					String parent = splitInHalf[0];
					String[] children = splitInHalf[1].split(",");
					for (String child : children)
					{
						h.add(map.hash(child), map.hash(parent));
					}
				}
				reader.close();
			} catch (IOException exception)
			{
				System.out.println("Something went wrong with " + Hfilename);
			}
		}

		this.transactionHierarchy = new Hierarchy();
		String Tfilename = "hierarchies/th";
		try (BufferedReader reader = new BufferedReader(new FileReader(Tfilename)))
		{
			while ((line = reader.readLine()) != null)
			{
				String[] splitInHalf = line.split(":");
				String parent = splitInHalf[0];
				String[] children = splitInHalf[1].split(",");
				for (String child : children)
				{
					this.transactionHierarchy.add(this.transactionMap.hash(child), this.transactionMap.hash(parent));
				}
			}
			reader.close();
		} catch (IOException exception)
		{
			System.out.println("Something went wrong with " + Tfilename);
		}

		for (Hierarchy hierarchy : this.categoricalHierarchies)
		{
			hierarchy.lock();
		}
		this.transactionHierarchy.lock();
		this.transactionSR = new BigDecimal(TWO.pow(this.transactionHierarchy.hierarchyLeaves())).subtract(BigDecimal.ONE);

		//categorical attributes first
		int swapPosition = 0;
		for (int i = 0; i < this.order.size(); i++)
		{
			if (this.order.get(i) == 'c')
			{
				double t = this.importance.remove(i);
				this.importance.add(swapPosition, t);
				swapPosition++;
			}
		}
	}

	public SI_Map transactionMap()
	{
		return this.transactionMap;
	}

	public Hierarchy transactionHierarchy()
	{
		return this.transactionHierarchy;
	}

	public void setImportance(List<Double> list)
	{
		this.importance = list;
	}

	public void setOrder(List<Character> list)
	{
		this.order = list;
		for (char character : this.order)
		{
			if (character == 'n')
			{
				this.ranges.add(new Range());
			}
			if (character == 'c')
			{
				this.categoricalMaps.add(new SI_Map());
			}
		}
	}

	public void setUlFunction(ULmethod method)
	{
		this.method = method;
	}

	public ULmethod getUlFunction()
	{
		return this.method;
	}

	public void setSplitter(String splitter)
	{
		this.splitter = splitter;
	}

	public void addGroup(Group g)
	{
		this.dataset.add(g);
	}

	public void setK(int k)
	{
		this.k = k;
	}

	public int getK()
	{
		return this.k;
	}

	public int getM()
	{
		return this.m;
	}

	public void setM(int m)
	{
		this.m = m;
	}

	public Group getGroup(int index)
	{
		return this.dataset.get(index);
	}

	public Group removeGroup(int index)
	{
		return this.dataset.remove(index);
	}

	public int numOfGroups()
	{
		return this.dataset.size();
	}

	@Override
	public String toString()
	{
		StringBuilder b = new StringBuilder();
		b.append("- - - - - - - - - - - - - - - - - - -\n");
		for (Group g : this.dataset)
		{
			b.append(g.toString());
			b.append("- - - - - - - - - - - - - - - - - - -\n");
		}
		return b.toString();
	}

	public BigDecimal ul() throws Exception
	{
		if (this.method.equals(ULmethod.pcta))
		{
			return ulPCTA();
		}

		if (this.method.equals(ULmethod.hierarchy))
		{
			return ulHierarchy();
		}
		throw new Exception("ul function is not set.");
	}

	public BigDecimal ul(Group group) throws Exception
	{
		if (this.method.equals(ULmethod.pcta))
		{
			return ulPCTA(group);
		}

		if (this.method.equals(ULmethod.hierarchy))
		{
			return ulHierarchy(group);
		}
		throw new Exception("ul function is not set.");
	}

	private BigInteger ulHierarchy(int item)
	{
		int u = this.transactionHierarchy.numOfLeaves(item);
		return TWO.pow(u).subtract(BigInteger.ONE);
	}

	private BigDecimal ulHierarchy(Instance instance)
	{
		BigInteger sum = BigInteger.ZERO;
		int bitCounter = -1;
		BitSet items = instance.getTra();
		while ((bitCounter = items.nextSetBit(bitCounter + 1)) != -1)
		{
			sum = sum.add(ulHierarchy(bitCounter));
		}
		BigDecimal b = new BigDecimal(sum);
		return b.divide(this.transactionSR, 100, RoundingMode.UP);

	}

	private BigDecimal ulHierarchy(Group group)
	{
		Group clone = new Group(group);
		Apriori apriori = new Apriori(this.transactionHierarchy, this.k, this.m);
		apriori.km(clone);
		BigDecimal sum = BigDecimal.ZERO;
		for (int i = 0; i < clone.size(); i++)
		{
			Instance instance = clone.getInstance(i);
			BigDecimal value = Dataset.this.ulHierarchy(instance);
			sum = sum.add(value);
		}
		BigDecimal size = new BigDecimal(String.valueOf(numOfInstances()));
		return sum.divide(size, 200, RoundingMode.UP);
	}

	private BigDecimal ulHierarchy()
	{
		BigDecimal result = BigDecimal.ZERO;
		for (int i = 0; i < this.dataset.size(); i++)
		{
			result = result.add(Dataset.this.ulHierarchy(this.dataset.get(i)));
		}
		return result;
	}

	private BigDecimal ulPCTA()
	{
		BigDecimal result = BigDecimal.ZERO;
		for (int i = 0; i < this.dataset.size(); i++)
		{
			result = result.add(ulPCTA(this.dataset.get(i)));
		}
		return result;
	}

	private BigDecimal ulPCTA(Group group)
	{
		Group clone = new Group(group);
		PCTA pcta = new PCTA(clone, this.k, this.m);
		pcta.pcta();
		pcta.applyRules();
		BigDecimal sum = BigDecimal.ZERO;
		for (int i = 0; i < clone.size(); i++)
		{
			Instance instance = clone.getInstance(i);
			BigDecimal value = ulPCTA(instance);
			sum = sum.add(value);
		}
		BigDecimal size = new BigDecimal(String.valueOf(numOfInstances()));
		return sum.divide(size, 200, RoundingMode.UP);
	}

	private BigDecimal ulPCTA(Instance instance)
	{
		BigInteger sum = BigInteger.ZERO;
		List<BitSet> list = instance.getTraList();
		for (BitSet itemset : list)
		{
			sum = sum.add(ulPCTA(itemset));
		}
		BigDecimal b = new BigDecimal(sum);
		return b.divide(this.transactionSR, 200, RoundingMode.UP);
	}

	private BigInteger ulPCTA(BitSet itemset)
	{
		int size = itemset.cardinality();
		if (size > 1)
		{
			return TWO.pow(itemset.cardinality()).subtract(BigInteger.ONE);
		} else
		{
			return BigInteger.ZERO;
		}
	}

	public double btd(Group a, Group b)
	{
		Group group = new Group();
		for (int i = 0; i < a.size(); i++)
		{
			group.addInstance(a.getInstance(i));
		}
		for (int i = 0; i < b.size(); i++)
		{
			group.addInstance(b.getInstance(i));
		}

		double max = 0;
		for (int i = 0; i < group.size(); i++)
		{
			for (int j = i + 1; j < group.size(); j++)
			{
				double btd = btd(group.getInstance(i).getTra(), group.getInstance(j).getTra());
				max = Double.max(max, btd);
			}
		}
		return max;
	}

	public double btd(BitSet a, BitSet b)
	{
		BitSet xor = (BitSet) a.clone();
		BitSet and = (BitSet) a.clone();
		BitSet or = (BitSet) a.clone();
		xor.xor(b);
		and.and(b);
		or.or(b);

		return (xor.cardinality() + 1) * or.cardinality() / (double) (and.cardinality() + 1);
	}

	public Instance generalize(Group group)
	{
		if (group.isGeneralized())
		{
			return group.getInstance(0);
		}

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
				int lca = this.categoricalHierarchies.get(j).lca(instance.getCat(j), temp.getCat(j));
				instance.set(j, lca);
			}
		}
		return instance;
	}

	public void anonymize(Group group)
	{
		if (group.isGeneralized())
		{
			return;
		}
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

	public double ncp(Instance a, Instance b)
	{
		double result = 0;
		for (int i = 0; i < a.catSize(); i++)
		{
			Hierarchy hierarchy = this.categoricalHierarchies.get(i);
			if(hierarchy.lca(a.getCat(i), b.getCat(i)) == null)
			{
				System.out.println(a.getCat(i));
				System.out.println(b.getCat(i));
				System.out.println(this.categoricalMaps.get(i).reverse().get(a.getCat(i)));
				System.out.println(this.categoricalMaps.get(i).reverse().get(b.getCat(i)));
			}
			int temp = hierarchy.numOfLeaves(hierarchy.lca(a.getCat(i), b.getCat(i)));
			if (temp > 1)
			{
				result += this.importance.get(i) * temp / hierarchy.hierarchyLeaves();
			}
		}

		for (int i = 0; i < a.numSize(); i++)
		{
			NumAttribute numAttribute = new NumAttribute(a.getNum(i));
			numAttribute.extend(b.getNum(i));
			double range = numAttribute.range();
			if (range > 0)
			{
				result += this.importance.get(a.catSize() + i) * range / this.ranges.get(i).range();
			}
		}
		return result;
	}

	public double ncp(Instance e)
	{
		double result = 0;
		for (int i = 0; i < e.catSize(); i++)
		{
			Hierarchy hierarchy = this.categoricalHierarchies.get(i);
			Integer temp = hierarchy.leaves(e.getCat(i)).size();
			if (temp > 1)
			{
				result += this.importance.get(i) * temp / hierarchy.hierarchyLeaves();
			}
		}

		for (int i = 0; i < e.numSize(); i++)
		{
			double range = e.getNum(i).range();
			if (range > 0)
			{
				result += this.importance.get(e.catSize() + i) * range / this.ranges.get(i).range();
			}
		}
		return result;
	}

	public Instance merge(Instance a, Instance b)
	{
		Instance result = new Instance();

		for (int i = 0; i < a.catSize(); i++)
		{
			Integer n = this.categoricalHierarchies.get(i).lca(a.getCat(i), b.getCat(i));
			result.add(n);
		}

		for (int i = 0; i < a.numSize(); i++)
		{
			NumAttribute temp = new NumAttribute(a.getNum(i));
			temp.extend(b.getNum(i));
			result.add(temp);
		}

		return result;
	}

	//supposed g is generalized
	public Double ncp(Group group)
	{
		if (group.isGeneralized())
		{
			Instance instance = group.getInstance(0);
			double ncp = ncp(instance);
			return ncp * group.size() / numOfInstances();
		}
		return null;
	}

	public double ncp()
	{
		double result = 0;
		for (Group group : this.dataset)
		{
			try
			{
				result += ncp(group);
			} catch (Exception ex)
			{
				System.out.println(ex);
			}
		}
		return result;
	}

	public int numOfInstances()
	{
		int counter = 0;
		for (Group group : this.dataset)
		{
			counter += group.size();
		}
		return counter;
	}
}
