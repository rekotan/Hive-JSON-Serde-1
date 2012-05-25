/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.openx.data.jsonserde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONArray;
import org.openx.data.jsonserde.json.JSONException;
import org.openx.data.jsonserde.json.JSONObject;

/**
 *
 * @author rcongiu
 */
public class JsonSerDeTest {
    
    public JsonSerDeTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws Exception {
        initialize();
    }
    
    @After
    public void tearDown() {
    }

    static JsonSerDe instance;
    
    static String defaultColumns = "one,two,three,four";
    static String defaultColumnTypes = "int,boolean,array<string>,string";

    static public void initialize() throws Exception {
        System.out.println("initialize");
        instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, "one,two,three,four");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "boolean,float,array<string>,string");
        
        instance.initialize(conf, tbl);
    }
    static public void alternateInitialize(String cols, String colTypes) throws Exception{
        System.out.println("initialize");
        instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, cols);
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, colTypes);
        
        instance.initialize(conf, tbl);   
    }

    @Test(expected=JSONException.class)
    public void testShouldDropValuesWhenWrongBasicType() throws Exception{
        alternateInitialize(defaultColumns, defaultColumnTypes);
        Writable w = new Text("{\"one\":true, \"two\": true}");
        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("two"), true);
        result.get("one");
    }

    @Test
    public void testShouldntDropFieldWhenExpectsString() throws Exception{
        alternateInitialize(defaultColumns, "string,string,array<string>,string");
        Writable w = new Text("{\"one\":23, \"two\": true}");
        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("two"), true);
        assertEquals(result.get("one"), 23L);
    }

    @Test
    public void testColumnNameReplacements() throws Exception{
        System.out.println("initialize");
        instance = new JsonSerDe();
        Configuration conf = null;
        String cols = "dt,from_field,name,age";
        String colTypes = "string,string,string,string";
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, cols);
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, colTypes);
        tbl.setProperty(JsonSerDe.PROP_COLUMN_MAPPINGS, "date:dt, from: from_field");
        instance.initialize(conf, tbl);
        Writable w = new Text("{\"date\":\"2011-10-12\",\"from\":\"matthew\"}");
        JSONObject result = (JSONObject) instance.deserialize(w);
        System.out.println(result.toString());
        assertEquals(result.get("dt"), "2011-10-12");
        assertEquals(result.get("from_field"), "matthew");
    }

    /**
     * Test of deserialize method, of class JsonSerDe.
     */
    @Test
    public void testDeserialize() throws Exception {
        System.out.println("deserialize");
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",\"orange\"],\"two\":19.5,\"four\":\"poop\"}");
        Object expResult = null;
        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"),"poop");
        
        assertTrue( result.get("three") instanceof JSONArray);
        
        assertTrue( ((JSONArray)result.get("three")).get(0) instanceof String );
        assertEquals( ((JSONArray)result.get("three")).get(0),"red");
    }

 //   {"one":true,"three":["red","yellow",["blue","azure","cobalt","teal"],"orange"],"two":19.5,"four":"poop"}

    @Test
    public void testDeserialize2() throws Exception {
        Writable w = new Text("{\"one\":true,\"three\":[\"red\",\"yellow\",[\"blue\",\"azure\",\"cobalt\",\"teal\"],\"orange\"],\"two\":19.5,\"four\":\"poop\"}");
        Object expResult = null;
        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("four"),"poop");
        
        assertTrue( result.get("three") instanceof JSONArray);
        
        assertTrue( ((JSONArray)result.get("three")).get(0) instanceof String );
        assertEquals( ((JSONArray)result.get("three")).get(0),"red");
    }
    
    @Test
    public void testDeserialize3() throws Exception {
        instance = new JsonSerDe();
        Configuration conf = null;
        String cols = "_id,assoc_id,four,year";
        String colTypes = "string,string,string,string";
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, cols);
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, colTypes);
        tbl.setProperty(JsonSerDe.PROP_REPLACE_STRINGS, "ObjectId\\(\\s*([\"a-zA-Z0-9]*)\\s*\\),Date\\(\\s*([0-9]*)\\s*\\)");
        instance.initialize(conf, tbl);
        Writable w = new Text("{\"_id\" : ObjectId( \"1a2b3c4d5e\" ), \"assoc_id\" : ObjectId( \"6f7g8h9i0j\" ), \"four\" : \"poop\", \"year\" : Date(18446743969597551616) }");
        JSONObject result = (JSONObject) instance.deserialize(w);
        assertEquals(result.get("_id"),"1a2b3c4d5e");
        assertEquals(result.get("assoc_id"),"6f7g8h9i0j");
        assertEquals(result.get("four"),"poop");
        assertEquals(result.get("year"),"18446743969597551616");
    }
    
    @Test
    public void testDeserialize4() throws Exception {
        instance = new JsonSerDe();
        Configuration conf = null;
        String cols = "num_bigint,num_int,num_float,num_double,params";
        String colTypes = "bigint,int,float,double,struct<num_bigint:bigint,num_double:double>";
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, cols);
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, colTypes);
        instance.initialize(conf, tbl);
        Writable w = new Text("{\"num_bigint\":1,\"num_int\":2,\"num_float\":3.1,\"num_double\":4.1," +
        		"\"params\":{\"num_bigint\":5,\"num_double\":8.1}}");
        JSONObject result = (JSONObject) instance.deserialize(w);
        System.out.println(result);
        assertEquals(result.get("num_bigint"),1L);
        assertEquals(result.get("num_bigint").getClass(),Long.class);
        assertEquals(result.get("num_int"),2);
        assertEquals(result.get("num_int").getClass(),Integer.class);
        assertEquals(result.get("num_float"),3.1f);
        assertEquals(result.get("num_float").getClass(),Float.class);
        assertEquals(result.get("num_double"),4.1d);
        assertEquals(result.get("num_double").getClass(),Double.class);
        assertEquals(((JSONObject)result.get("params")).get("num_bigint"),5L);
        assertEquals(((JSONObject)result.get("params")).get("num_bigint").getClass(),Long.class);
        assertEquals(((JSONObject)result.get("params")).get("num_double"),8.1d);
        assertEquals(((JSONObject)result.get("params")).get("num_double").getClass(),Double.class);
    }
    
    /**
     * Test of getSerializedClass method, of class JsonSerDe.
     */
    @Test
    public void testGetSerializedClass() {
        System.out.println("getSerializedClass");
        Class expResult = Text.class;
        Class result = instance.getSerializedClass();
        assertEquals(expResult, result);
       
    }

    @Test
    public void testDeserializeWithBase16Numbers() throws Exception{
        System.out.println("deserialization craziness test:");
        String data = "{\"hello\":\"\\uA8?1\"}";
        JSONObject j = new JSONObject(data);
        assertEquals(j.get("hello"), "\\uA8?1");
    }

    /**
     * Test of serialize method, of class JsonSerDe.
     */
/*    @Test
    public void testSerialize() throws Exception {
        System.out.println("serialize");
        Object o = null;
        ObjectInspector oi = null;
        JsonSerDe instance = new JsonSerDe();
        Writable expResult = null;
        Writable result = instance.serialize(o, oi);
        assertEquals(expResult, result);
    }
     *  
     */
    
    
   // @Test
    public void testSerialize() throws SerDeException, JSONException {
        System.out.println("serialize");
        ArrayList row = new ArrayList(5);
        
        List<ObjectInspector> lOi = new LinkedList<ObjectInspector>();
        List<String> fieldNames = new LinkedList<String>();
        
        row.add("HELLO");
        fieldNames.add("atext");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        row.add(10);
        fieldNames.add("anumber");
        lOi.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        
        List<String> array = new LinkedList<String>();
        array.add("String1");
        array.add("String2");
        
        row.add(array);
        fieldNames.add("alist");
        lOi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));
        
        Map<String,String> m = new HashMap<String,String>();
        m.put("k1","v1");
        m.put("k2","v2");
        
        row.add(m);
        fieldNames.add("amap");
        lOi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA),
                ObjectInspectorFactory.getReflectionObjectInspector(String.class, 
                   ObjectInspectorFactory.ObjectInspectorOptions.JAVA)));
        
        
        StructObjectInspector soi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, lOi);
        
        Object result = instance.serialize(row, soi);
        
        JSONObject res = new JSONObject(result.toString());
        assertEquals(res.getString("atext"), row.get(0));
        
        assertEquals(res.get("anumber") , row.get(1));
        
        // after serialization the internal contents of JSONObject are destroyed (overwritten by their string representation
       // (for map and arrays) 
      
       
        System.out.println("Serialized to " + result.toString());
        
    }
    
 }
