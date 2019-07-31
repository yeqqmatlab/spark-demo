案例数据：

employees.json

```
{"name":"Michael", "salary":3000}{"name":"Andy", "salary":4500}{"name":"Justin", "salary":3500}{"name":"Berta", "salary":4000}
```

people.json

```json
{"name":"Michael"}{"name":"Andy", "age":30}{"name":"Justin", "age":19}
```

hello.txt

```txt
hello world kafka spark 
flume sqoopflume sparkstreaming 
sparksql hive azkabanflume world 
hello kafka hbase sqoop
```

people.txt

```
Michael, 29
Andy, 30
Justin, 19
```

核心依赖：

```xml
    <groupId>org.apache.spark</groupId>    
    <artifactId>spark-core_2.11</artifactId>    
    <version>2.2.0</version>
</dependency>
<dependency>    
    <groupId>org.apache.hadoop</groupId>    
    <artifactId>hadoop-client</artifactId>    
    <version>2.7.4</version>
</dependency>
```



一 ： 创建RDD



SparkConf conf = new SparkConf().setAppName(AppName).setMaster("local[2]");
JavaSparkContext sc = new JavaSparkContext(conf);

1 并行化创建rdd

```java
List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
JavaRDD<Integer> distData = sc.parallelize(data);
Integer reduce = distData.reduce((a, b) -> a + b);//        System.out.println(reduce);
```

2 读取本地文件

```java
JavaRDD<String> distFile = sc.textFile("data.txt");
Integer reduce1 = distFile.map(s -> s.length()).reduce((a, b) -> a + b);
System.out.println(reduce1);
```

3 读取hdfs

```java
JavaRDD<String> distFile1 = sc.textFile("hdfs://ip243:8020/test/words.txt");
Integer reduce2 = distFile1.map(s -> s.length()).reduce((a, b) -> (a + b));
System.out.println(reduce2);
```

4 读取文件夹

```java
//读取多个文件,windows下添加具体文件，不报错，添加文件夹，报错       
JavaPairRDD<String, String> data = sc.wholeTextFiles("data/");
JavaRDD<String> fileNameString = data.map(new Function<Tuple2<String, String>, String>() {
	@Override
	public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
	return stringStringTuple2._2;
	}
});
List<String> collect = fileNameString.collect();
for(int i = 0;i < collect.size();i++){
	System.out.println(collect.get(i));
	}
```



5 读取sequenceFile 

```java
JavaPairRDD<Text, IntWritable> sequenceFile = sc.sequenceFile("", Text.class, IntWritable.class);
```

6 读取objectFile

```java
JavaRDD<Object> object = sc.objectFile("");
```



二 RDD常用算子

map以及filter算子

lamda表达式

```java
List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
JavaRDD<Integer> rdd = sc.parallelize(list);
JavaRDD<Integer> rdd1 = rdd.map(x->x*2);
JavaRDD<Integer> filter = rdd1.filter(x -> x > 9);
filter.foreach(x-> System.out.println(x));
```

匿名函数写法：

```java
List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
//初始化rdd
JavaRDD<Integer> rdd = sc.parallelize(list);JavaRDD<Integer> rdd1 = rdd.map(
    new Function<Integer, Integer>() {    
        @Override    
        public Integer call(Integer integer) throws Exception {        
            int i = integer * 2;        
            return i;    
        }
    });
//map操作
JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });
//filter操作
JavaRDD<Integer> filter = rdd1.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer > 9;
            }
        });
//打印操作
filter.foreach(new VoidFunction<Integer>() {   
    @Override    
    public void call(Integer integer) throws Exception {        					System.out.println(integer);                                                
    }});
```

flatmap算子

lamda表达式

```java
JavaRDD<String> lineRdd = sc.textFile("hello.txt"); 
JavaRDD<String> wordRdd = lineRdd.flatMap(
    x -> Arrays.asList(x.split(" ")).iterator()
);
wordRdd.foreach(x-> System.out.println(x));
```

匿名函数类：

```java
//初始化rdd
JavaRDD<String> lineRdd = sc.textFile("hello.txt");
//map操作
JavaRDD<String[]> strings = lineRdd.map(new Function<String, String[]>() {    
    @Override    
    public String[] call(String s) throws Exception {       
        return s.split(" ");    
    }
});
//flatmap操作
JavaRDD<String> wordRdd = strings.flatMap(
    new FlatMapFunction<String[], String>() {   
        @Override    
        public Iterator<String> call(String[] strings) throws Exception {       
            Iterator<String> iterator = Arrays.asList(strings).iterator();     
            return iterator;   
        }
    });
//打印操作
wordRdd.foreach(x-> System.out.println(x));
```





union&intersection&distinct

```java
List<Integer> list1 = Arrays.asList(5, 6, 4, 3);
List<Integer> list2 = Arrays.asList(1, 2, 3, 4);
//并行化集合,获取初始
RDDJavaRDD<Integer> rdd1 = sc.parallelize(list1);
JavaRDD<Integer> rdd2 = sc.parallelize(list2);
//union 求并集，不去除重复
JavaRDD<Integer> union = rdd1.union(rdd2);
//求交集
JavaRDD<Integer> intersection = rdd1.intersection(rdd2);
//去重
JavaRDD<Integer> distinct = rdd1.distinct();
//rdd foreach
union.foreach(x-> System.out.println(x));
```

join&groupbykey

```java
//初始化集合
List<Tuple2<String, Integer>> list1 = Arrays.asList(        
new Tuple2<String, Integer>("tom", 1),        
new Tuple2<String, Integer>("jerry", 3),        
new Tuple2<String, Integer>("kitty", 2));
List<Tuple2<String, Integer>> list2 = Arrays.asList(        
new Tuple2<String, Integer>("jerry", 2),        
new Tuple2<String, Integer>("tom", 1),        
new Tuple2<String, Integer>("shuke", 2));

//并行化集合获取初始
RDDJavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(list1);
JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(list2);
//join 操作，结果是String, Tuple2<Integer, Integer>

JavaPairRDD<String, Tuple2<Integer, Integer>> join = rdd1.join(rdd2);
//join.foreach(x-> System.out.println(x._1+":"+x._2._1+" "+x._2._2));

//union 操作 不去重
JavaPairRDD<String, Integer> union = rdd1.union(rdd2);
//union.foreach(x-> System.out.println(x._1+":"+x._2));

//groupbykey 操作 结果是String, Iterable<Integer>
JavaPairRDD<String, Iterable<Integer>> groupByKey = union.groupByKey();

//将groupbykey结果的value值进行求和
JavaPairRDD<String, Integer> integersToInteger = groupByKey.mapValues(
    new Function<Iterable<Integer>, Integer>() {    
        @Override    
        public Integer call(Iterable<Integer> integers) throws Exception {     
            Iterator<Integer> iterator = integers.iterator();        
            int i = 0;       
            while (iterator.hasNext()) {            
                i += iterator.next();       
            }        
            return i;    
        }
    });

//打印操作
integersToInteger.foreach(x-> System.out.println(x._1+":"+x._2));
```



reducebykey

```java
//上述union
JavaPairRDD<String, Integer> union = rdd1.union(rdd2);

//reducebykey  lamda表达式
JavaPairRDD<String, Integer> reduceByKey = union.reduceByKey((a, b) -> a + b);
reduceByKey.foreach(x-> System.out.println(x._1+":"+x._2));

//reducebykey  匿名函数
JavaPairRDD<String, Integer> reduceByKey1 = union.reduceByKey(
    new Function2<Integer, Integer, Integer>() {           
        @Override           
        public Integer call(Integer integer, Integer integer2) throws Exception 
        {
                
            int i = integer + integer2;  
            return i;
            }
        });
```





sortbykey

```java
JavaPairRDD<String, Integer> sortByKey = reduceByKey.sortByKey();
```

coalesce&repartition

```java
System.out.println(union.coalesce(2).partitions().size());  //减少分区
System.out.println(union.repartition(4).partitions().size());  //可增可减
```

三: sparkSQL

核心依赖

```xml
<dependency>    
    <groupId>org.apache.spark</groupId>    
    <artifactId>spark-sql_2.11</artifactId>    
    <version>2.2.0</version>
</dependency>
```



```java
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession
  .builder()
  .appName("Java Spark SQL basic example")
  .master("local")
  .getOrCreate();
```

1 创建dataset<Row>

```java
DataSet<Row> df = spark.read().json("people.json");
// Displays the content of the DataFrame to stdout
df.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

2 创建局部临时表（session会话中）

```
df.createOrReplaceTempView("people");

Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
sqlDF.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

3 创建全局临时表

```java
// Register the DataFrame as a global temporary view
df.createGlobalTempView("people");

// Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

2 创建dataset<Person>

```java
import org.apache.spark.sql.*;
import java.io.Serializable;
import java.util.Collections;
public class SparkSQLDemo {    
    public static void main(String[] args) {
        //创建sparksession
        SparkSession spark = SparkSession                
            .builder()                
            .appName("Java Spark SQL basic example")                
            .master("local")                
            .getOrCreate();        
        // Create an instance of a Bean class        
        Person person = new Person();        
        person.setName("Andy");        
        person.setAge(32);        
        // Encoders are created for Java beans        
        Encoder<Person> personEncoder = Encoders.bean(Person.class);        
        Dataset<Person> javaBeanDS = spark.createDataset(               
            Collections.singletonList(person),                
            personEncoder        
        );        
        javaBeanDS.show();    
    }    
    
    //静态内部类作为样例类
    public static class Person implements Serializable {        
        private String name;        
        private int age;        
        public String getName() {            
            return name;        
        }        
        public void setName(String name) {            
            this.name = name;        
        }        
        public int getAge() {            
            return age;        
        }        
        public void setAge(int age) {            
            this.age = age;        
        }    
    }
}
+---+----+
|age|name|
+---+----+
| 32|Andy|
+---+----+
```

3 创建dataset<Integer>

```java
//创建编码器
Encoder<Integer> integerEncoder = Encoders.INT();
//创建dataset<integer>
Dataset<Integer> primitiveDS = spark.createDataset(
    Arrays.asList(1, 2, 3), 
    integerEncoder
    );
//dataset map操作
Dataset<Integer> transformedDS = primitiveDS.map(        
    (MapFunction<Integer, Integer>) value -> value + 1,        
    integerEncoder
    );
//收集到driver节点
transformedDS.collect(); // Returns [2, 3, 4]
```



四 由dataset<string>创建dataset<row>



4.1 通过反射创建dataset<row>

```java
//创建javardd<person>       
JavaRDD<Person> peopleRDD = spark.read()                
    .textFile("people.txt")                
    .javaRDD()                
    .map(                        
    line -> {                            
        String[] parts = line.split(",");                            
        Person person = new Person();                            
        person.setName(parts[0]);                            
        person.setAge(Integer.parseInt(parts[1].trim()));                            
        return person;                
    }
); 


//将rdd<person> 转化为dataset<row>      
Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);        


// 创建临时表      
peopleDF.createOrReplaceTempView("people");        


// 将name 选出来      
Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 
                                     19");        
//创建字符串编码器     
Encoder<String> stringEncoder = Encoders.STRING();  
                                     
//将dataset<row> 转化为dataset<string>                                   
Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(                
    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),                
    stringEncoder        
    
    );        

//展示                                     
teenagerNamesByIndexDF.show();

                                     
                                           
//通过字段将dataset<row>转化为dataset<String>                                  
Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(                
    (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),               
    stringEncoder
    );        

//展示
teenagerNamesByFieldDF.show();
```





4.2 通过接口自定义schema ,创建dataset<row>

```java
//自定义schema,字段名
String schemaString = "name age";
//创建list,存放structfield
ArrayList<StructField> structFields = new ArrayList<>();
//将每一个字段构造structField,存放到集合中
for (String field : schemaString.split(" ")) {    
    StructField structField = DataTypes.createStructField(field, DataTypes.StringType, true);    
    structFields.add(structField);
}
StructType schema = DataTypes.createStructType(structFields);




Dataset<String> dataset = spark.read().textFile("people.txt");
JavaRDD<String> javaRDD = dataset.toJavaRDD();
//将rdd<string> 转变为rdd<row>
JavaRDD<Row> rowJavaRDD = javaRDD.map(        
    (Function<String, Row>) str -> {            
        String[] split = str.split(" ");            
        Row row = RowFactory.create(split[0], split[1]);            
        return row;        
    });
//将rdd<row>转变为dataset<row>
Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, schema);
```





五 sparksql udaf 函数

```java
//自定义求均值函数

public static class MyAverage extends UserDefinedAggregateFunction {    
    //输入数据schema    
    private StructType inputSchema;    
    //缓存数据schema    
    private StructType bufferSchema;   
    public MyAverage() {        
        List<StructField> inputFields = new ArrayList<>();        
        inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));        
        inputSchema = DataTypes.createStructType(inputFields);        
        List<StructField> bufferFields = new ArrayList<>();        
        
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));   
        bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true)); 
        bufferSchema = DataTypes.createStructType(bufferFields);    }    
    // 输入数据    
    public StructType inputSchema() {        
        inputSchema;    
    }    
    // 缓存数据    
    public StructType bufferSchema() {        
        return bufferSchema;    
    }    
    // 输出数据    
    public DataType dataType() {        
        return DataTypes.DoubleType;    
    }    
    //输入相同，输出是否相同    
    public boolean deterministic() {        
        return true;    }        
    //初始化缓存    
    public void initialize(MutableAggregationBuffer buffer) {       
        buffer.update(0, 0L);       
        buffer.update(1, 0L);    
    }       
    //缓存更新    
    public void update(MutableAggregationBuffer buffer, Row input) {        
        if (!input.isNullAt(0)) {            
            long updatedSum = buffer.getLong(0) + input.getLong(0);           
            long updatedCount = buffer.getLong(1) + 1;          
            buffer.update(0, updatedSum);           
            buffer.update(1, updatedCount);       
        }   
    }        
    //合并各个节点的缓存    
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {       
        long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);      
        long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);       
        buffer1.update(0, mergedSum);     
        buffer1.update(1, mergedCount);   
    }    
    // 计算最终结果    
    public Double evaluate(Row buffer) {       
        return ((double) buffer.getLong(0)) / buffer.getLong(1);  
    }
}
```





```java
// Register the function to access it
spark.udf().register("myAverage", new MyAverage());


Dataset<Row> df = spark.read().json("employees.json");
df.createOrReplaceTempView("employees");
df.show();
// +-------+------+
// |   name|salary|
// +-------+------+
// |Michael|  3000|
// |   Andy|  4500|
// | Justin|  3500|
// |  Berta|  4000|
// +-------+------+

Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
result.show();
// +--------------+
// |average_salary|
// +--------------+
// |        3750.0|
// +--------------+
```