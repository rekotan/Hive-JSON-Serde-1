package org.openx.data.jsonserde.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * A JavaLongObjectInspector inspects a Java Long Object.
 */
public class CustomJavaLongObjectInspector extends
		AbstractPrimitiveJavaObjectInspector implements
		SettableLongObjectInspector {

	CustomJavaLongObjectInspector() {
		super(PrimitiveObjectInspectorUtils.longTypeEntry);
	}

	@Override
	public Object getPrimitiveWritableObject(Object o) {
		return o == null ? null : new LongWritable(((Long) o).longValue());
	}

	@Override
	public long get(Object o) {
		if (o instanceof Integer) {
			return ((Integer) o).longValue();
		} else {
			return ((Long) o).longValue();
		}
	}

	@Override
	public Object create(long value) {
		return Long.valueOf(value);
	}

	@Override
	public Object set(Object o, long value) {
		return Long.valueOf(value);
	}

}
