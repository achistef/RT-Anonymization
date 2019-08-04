package metric;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;
import scala.Tuple4;
import struct.Group;

/**
 *
 * @author achilles
 */
public class RatioGroupCalculator implements Serializable, Function<Tuple3<Double, BigDecimal, Group>, Tuple4<Double, BigDecimal, Group, BigDecimal>>
{

	@Override
	public Tuple4<Double, BigDecimal, Group, BigDecimal> call(Tuple3<Double, BigDecimal, Group> t1) throws Exception
	{
		BigDecimal d = t1._2().compareTo(BigDecimal.ZERO) != 0? 
			new BigDecimal(t1._1()).divide(t1._2(), 200, RoundingMode.CEILING) : new BigDecimal("100000000000");
		return new Tuple4<>(t1._1(), t1._2(), t1._3(), d);
		//ncp loss / utility loss
	}

}
