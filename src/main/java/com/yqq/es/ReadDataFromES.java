package com.yqq.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yqq on 2019/11/12.
 */
public class ReadDataFromES {

    public static void main(String[] args) throws java.io.IOException, URISyntaxException {

        SparkConf conf = new SparkConf()
                .setAppName("ElasticSearch-spark")
                .setMaster("local[2]")
                .set("es.es.index.auto.create", "true")
                .set("es.nodes","192.168.1.243")
                .set("es.port","9200")
                .set("es.nodes.wan.only", "true");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
        JavaRDD<Map<String, Object>> searchRdd = JavaEsSpark.esRDD(jsc, "zsy-etl" ).values();

        /*for (Map<String, Object> item : searchRdd.collect()) {
            item.forEach((key, value)->{
                System.out.println("key:" + key + ", value:" + value);

            });
        }*/

        //展开成一维数据
        JavaRDD<Row> rowJavaRDD = searchRdd.map(mapObject -> {

            JSONObject object = new JSONObject(mapObject);
            Long handleTime = object.getLong("handleTime");
            String continentName = object.getJSONObject("geoip").getString("continent_name");
            String cityName = object.getJSONObject("geoip").getString("city_name");
            String countryName = object.getJSONObject("geoip").getString("country_name");
            String countryCode2 = object.getJSONObject("geoip").getString("country_code2");
            String continentCode = object.getJSONObject("geoip").getString("continent_code");
            String regionName = object.getJSONObject("geoip").getString("region_name");
            Float lon = object.getJSONObject("geoip").getJSONObject("location").getFloat("lon");
            Float lat = object.getJSONObject("geoip").getJSONObject("location").getFloat("lat");
            String regionCode = object.getJSONObject("geoip").getString("region_code");
            String requestMethod = object.getString("requestMethod");
            String serviceGroup = object.getString("serviceGroup");
            String requestIp = object.getString("requestIp");
            String userAgent = object.getString("userAgent");
            String userId = object.getString("userId");
            String timestamp = object.getString("@timestamp");
            String service = object.getString("service");
            String requestUrl = object.getString("requestUrl");
            String errCode = object.getString("errCode");
            String schoolId = object.getString("schoolId");
            String client = object.getString("client");
            return RowFactory.create(handleTime, continentName, cityName, countryName, countryCode2, continentCode, regionName, lon, lat, regionCode, requestMethod, serviceGroup, requestIp, userAgent, userId, timestamp, service, requestUrl, errCode, schoolId, client);
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowJavaRDD, getSchema());
        dataFrame.show();

        //转成parquet格式，写入hdfs上
        final FileSystem hdfs = FileSystem.get(new URI("hdfs://ip243:8020"), new Configuration());
        String outputPath = "hdfs://ip243:8020//zsy/warehouse3/log_data";
        if(hdfs.exists(new Path(outputPath))){
            System.out.println("delete exits files");
            hdfs.delete(new Path(outputPath),true);
        }
        dataFrame.write().parquet(outputPath);

        sparkSession.stop();
    }

    /**
     * 构造表结构
     * @return
     */
    private static StructType getSchema(){

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("handle_time",DataTypes.LongType,true));
        fields.add(DataTypes.createStructField("continent_name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("country_name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("country_code2",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("continent_code",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("region_name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("lon",DataTypes.FloatType,true));
        fields.add(DataTypes.createStructField("lat",DataTypes.FloatType,true));
        fields.add(DataTypes.createStructField("region_code",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("request_method",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("service_group",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("request_ip",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("user_agent",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("user_id",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("timestamp",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("service",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("request_url",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("err_code",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("school_id",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("client",DataTypes.StringType,true));
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }
}
