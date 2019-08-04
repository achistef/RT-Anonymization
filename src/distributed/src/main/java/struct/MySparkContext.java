package struct;

import java.io.Serializable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author achilles
 */
public class MySparkContext implements Serializable
{

	private static final JavaSparkContext CONTEXT;

	static
	{
		//define spark context
		SparkConf conf = new SparkConf().setAppName("RT anonymization");
		CONTEXT = new JavaSparkContext(conf);
	}

	private MySparkContext()
	{
	}

	public static JavaSparkContext getSparkContext()
	{
		return CONTEXT;
	}

}
