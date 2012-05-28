package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;

/**
 * A JavaDoubleObjectInspector inspects a Java Double Object.
 */
public class CustomJavaDoubleObjectInspector extends
		AbstractPrimitiveJavaObjectInspector implements
		SettableDoubleObjectInspector {

	CustomJavaDoubleObjectInspector() {
		super(PrimitiveObjectInspectorUtils.doubleTypeEntry);
	}

	@Override
	public Object getPrimitiveWritableObject(Object o) {
		return o == null ? null
				: new DoubleWritable(((Double) o).doubleValue());
	}

	@Override
	public double get(Object o) {
		if (o instanceof Integer) {
			return ((Integer) o).doubleValue();
		} else if (o instanceof Long) {
			return ((Long) o).doubleValue();
		} else if (o instanceof Float) {
			return ((Float) o).doubleValue();
		} else {
			return ((Double) o).doubleValue();
		}
	}

	@Override
	public Object create(double value) {
		return Double.valueOf(value);
	}

	@Override
	public Object set(Object o, double value) {
		return Double.valueOf(value);
	}

}
