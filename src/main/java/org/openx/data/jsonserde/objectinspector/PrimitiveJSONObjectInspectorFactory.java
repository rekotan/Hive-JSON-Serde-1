package org.openx.data.jsonserde.objectinspector;

import java.util.HashMap;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public final class PrimitiveJSONObjectInspectorFactory {

	private static HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector> cachedInspectors = new HashMap<PrimitiveCategory, AbstractPrimitiveJavaObjectInspector>();
	static {
		cachedInspectors.put(PrimitiveCategory.STRING, new CustomJavaStringObjectInspector());
		cachedInspectors.put(PrimitiveCategory.LONG, new CustomJavaLongObjectInspector());
		cachedInspectors.put(PrimitiveCategory.FLOAT, new CustomJavaFloatObjectInspector());
		cachedInspectors.put(PrimitiveCategory.DOUBLE, new CustomJavaDoubleObjectInspector());
	}

	public static AbstractPrimitiveJavaObjectInspector getPrimitiveJavaObjectInspector(PrimitiveCategory cat) {
		AbstractPrimitiveJavaObjectInspector result = cachedInspectors.get(cat);
		if (result == null) {
			result = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(cat);
		}
		return result;
	}

}