package com.myz.hbaseA;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseMonth {
private static Configuration con;
//连接
static {
	con=HBaseConfiguration.create();
	con.set("hbase.zookeeper.quorum", "192.168.127.152");
	con.set("hbase.zookeeper.property.clientPort", "2181");
}
public static Connection getcon() throws IOException{
	return ConnectionFactory.createConnection(con);
}
//main方法调用
public static void main(String[] args) throws IOException {
	//创建命名空间
//	createName("month");
//	System.out.println("执行完成");
	//建表
//	createTable("month:t_student", "info");
//	System.out.println("t_student"+"完成");
//	
//	createTable("month:t_course", "info");
//	System.out.println("t_courset"+"完成");
//	
//	createTable("month:t_student_course", "info");
//	System.out.println("t_student_course"+"完成");
	//插入数据
//	insertStu();
//	insertCou();
//	insertSc();
	//（3）查询选择某个课程的学生信息（传入课程行键）
	getOneCourse("course201912241538149188");
}
//1.创建命名空间
public static void createName(String name) throws IOException{
	Admin admin = getcon().getAdmin();
	NamespaceDescriptor build = NamespaceDescriptor.create(name).build();
	admin.createNamespace(build);
}
//2.创建表
public static void createTable(String tableName,String... cfs) throws IOException{
	Admin admin = getcon().getAdmin();
	HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
	for (String cf : cfs) {
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes(cf));
		tableDescriptor.addFamily(columnDescriptor);
	}
	admin.createTable(tableDescriptor);
}
//3.插入数据
public static void putrows(String tableName,Map<String, Map<String, String>> data) throws IOException{
	HTable table = (HTable) getcon().getTable(TableName.valueOf(tableName));
	//put集合
	ArrayList<Put> list = new ArrayList<Put>();
	//ent
	Set<Entry<String,Map<String,String>>> entrySet = data.entrySet();
	//拆解
	for (Entry<String, Map<String, String>> entry : entrySet) {
		//获取key
		Put put = new Put(Bytes.toBytes(entry.getKey()));
		Map<String, String> value = entry.getValue();
		Set<Entry<String,String>> entrySet2 = value.entrySet();
		for (Entry<String, String> entry2 : entrySet2) {
			String key = entry2.getKey();
			put.addColumn(Bytes.toBytes(key.split(":")[0]), Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(entry2.getValue()));
		}
		list.add(put);
	}
	table.put(list);
}
//（2）向学生表插入10条数据
public static void insertStu() throws IOException{
	HashMap datamap = new HashMap();
	String[] address={"shanghai","beijing","shijiazhuang","tianjing","suqian","dalian","shangdong","shangxi"};
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	for (int i = 0; i < 10; i++) {
		HashMap cellmap = new HashMap();
		String date = format.format(new Date());
		int j=(int) ((Math.random()*9+1)*1000);
		int k=new Random().nextInt(8);
		String rowkey="student"+date+j;
		cellmap.put("info:name", "student"+i);
		cellmap.put("info:age", String.valueOf(18+i));
		cellmap.put("info:address", address[k]);
		datamap.put(rowkey, cellmap);
		putrows("month:t_student", datamap);
	}
	System.out.println("添加成功");
}
//向课程表插入5条数据
public static void insertCou() throws IOException{
	HashMap datamap = new HashMap();
	String[] course={"yuwen","shuxue","dili","shengwu","wuli"};
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
	for (int i = 0; i < 5; i++) {
		HashMap cellmap = new HashMap();
		String date = format.format(new Date());
		int j=(int) ((Math.random()*9+1)*1000);
//		int k=new Random().nextInt(8);
		String rowkey="course"+date+j;
		cellmap.put("info:name", course[i]);
		cellmap.put("info:teacher", "teacher"+i);
		datamap.put(rowkey, cellmap);
		putrows("month:t_course", datamap);
	}
	System.out.println("添加成功");
}
//向学生课程表插入30条数据
public static void insertSc() throws IOException{
	//集合
	ArrayList stulist = new ArrayList();
	ArrayList courselist = new ArrayList();
	
	//获取学生的行键
	HTable table1 = (HTable) getcon().getTable(TableName.valueOf("month:t_student"));
	ResultScanner scanner1 = table1.getScanner(new Scan());
	for (Result result : scanner1) {
		stulist.add(Bytes.toString(result.getRow()));
	}
	//课程的行键
	HTable table2 = (HTable) getcon().getTable(TableName.valueOf("month:t_course"));
	ResultScanner scanner2 = table2.getScanner(new Scan());
	for (Result result : scanner2) {
		courselist.add(Bytes.toString(result.getRow()));
	}
	//插入数据
	for (int i = 0; i < stulist.size(); i++) {
		HashSet<Integer> sets = new HashSet<Integer>();
		while(sets.size()<3){
			sets.add(new Random().nextInt(5));
		}
		for (Integer set : sets) {
			HashMap datamap = new HashMap();
			HashMap cellmap = new HashMap();
			cellmap.put("info:sturow", stulist.get(i));
			cellmap.put("info:courserow", courselist.get(set));
			datamap.put(stulist.get(i)+"_"+courselist.get(set), cellmap);
			putrows("month:t_student_course", datamap);
		}
	}
	System.out.println("中间表插入成功");
}
//（3）查询选择某个课程的学生信息
public static void getOneCourse(String courseid) throws IOException{
	HTable table = (HTable) getcon().getTable(TableName.valueOf("month:t_student_course"));
	SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("courserow"), CompareOp.EQUAL, Bytes.toBytes(courseid));
	Scan scan = new Scan();
	ResultScanner scanner = table.getScanner(scan);
	for (Result result : scanner) {
		Get get = new Get(Bytes.toBytes(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sturow")))));
		Table table2 = getcon().getTable(TableName.valueOf("month:t_student"));
		Result result2 = table2.get(get);
		System.out.println("表名:"+table2.getName());
		System.out.println("行键:"+Bytes.toString(result2.getRow()));
		System.out.println("姓名:"+Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
		System.out.println("年龄:"+Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))));
		System.out.println("住址:"+Bytes.toString(result2.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"))));
		System.out.println("------------------------------------");
	}
}
}
