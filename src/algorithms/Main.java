package algorithms;

import java.util.ArrayList;
import java.util.List;
import structures.Dataset;
import structures.ULmethod;

/**
 *
 * @author achilles
 */
public class Main
{

	public static void main(String[] Args) throws Exception
	{
		int attributes = 8;
		int cat = 6;

		//create attribute list 
		List<Character> order = new ArrayList<>();
		order.add('n');
		for (int i = 0; i < cat; i++)
		{
			order.add('c');
		}
		order.add('t');

		//create importance list
		List<Double> importance = new ArrayList<>();
		for (int i = 0; i < attributes - 1; i++)
		{
			importance.add(1.0);
		}

		String filename = "dataset";
		String splitter = ",";

		int k = 10;
		int m = 3;
		double d = 0.7;

		Dataset dataset = new Dataset();
		dataset.setOrder(order);
		dataset.setSplitter(splitter);
		dataset.setImportance(importance);
		dataset.read(filename);
		dataset.setK(k);
		dataset.setM(m);
		dataset.setUlFunction(ULmethod.hierarchy);

		Rum_Bound rb = new Rum_Bound(dataset);
		rb.set_d(d);

		if (rb.run())
		{
			System.out.println("OK");
		} else
		{
			System.out.println("dataset could not fit");
		}
		//dataset.saveAs("result");
		System.out.println("Results : ");
		System.out.println("Dataset ncp : " + dataset.ncp());
		System.out.println("Dataset ul : " + dataset.ul());
	}
}
