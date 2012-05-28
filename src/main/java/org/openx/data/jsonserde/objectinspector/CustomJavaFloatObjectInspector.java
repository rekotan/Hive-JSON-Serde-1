package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.io.FloatWritable;

/**
 * A JavaFloatObjectInspector inspects a Java Float Object.
 */
public class CustomJavaFloatObjectInspector extends
		AbstractPrimitiveJavaObjectInspector implements
		SettableFloatObjectInspector {

	CustomJavaFloatObjectInspector() {
		super(PrimitiveObjectInspectorUtils.floatTypeEntry);
	}

	@Override
	public Object getPrimitiveWritableObject(Object o) {
		return o == null ? null : new FloatWritable(((Float) o).floatValue());
	}

	@Override
	public float get(Object o) {
		if (o instanceof Integer) {
			return ((Integer) o).floatValue();
		} else if (o instanceof Long) {
			return ((Long) o).floatValue();
		} else if (o instanceof Double) {
			return ((Double) o).floatValue();
		} else {
			return ((Float) o).floatValue();
		}
	}

	@Override
	public Object create(float value) {
		return Float.valueOf(value);
	}

	@Override
	public Object set(Object o, float value) {
		return Float.valueOf(value);
	}

}
