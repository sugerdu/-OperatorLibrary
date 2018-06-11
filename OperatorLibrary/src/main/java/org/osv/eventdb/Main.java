package org.osv.eventdb;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class Main {
	public static void main(String args[]) throws Exception{
		//配置Hbase环境
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.master", "192.168.0.178:61000");
		String path = "hdfs://192.168.0.178:9000/user/OperatorLibrary.jar";
		//建立连接
		Connection conn = ConnectionFactory.createConnection(conf);
		// 输入runID
		@SuppressWarnings("resource")
		Scanner sc = new Scanner(System.in);
		System.out.println("请输入runID");
		String	runID = sc.nextLine();
		//输入property
		System.out.println("请输入property");
		String	property = sc.nextLine();
		// 输入startvalue
		System.out.println("请输入起始范围");
		String	startvalue = sc.nextLine();
		// 输入endvalue
		System.out.println("请输入终止范围");
		String	stopvalue = sc.nextLine();
		//1、开始获取data/执行ReadRoot
		//2、将数据存储Hbase表中（表名为：Coprocessor_Table）
		ArrayList<Double> datalist=new ArrayList<Double>();//定义存储结果的数据结构
		GetRootData GRD=new GetRootData();//创建ReadRoot对象
		UpData UD=new UpData();//创建UpData的对象
		datalist=GRD.getRootData(conf,conn, runID, property, startvalue, stopvalue);
		System.out.println(datalist);//输出数据
		System.out.println("是否建表(请输入yes or no)");
		String temp1=sc.nextLine();
		if(temp1.equals("no"))
		{
			return;
		}
		UD.creattable(conn);
		UD.adddata1(conf, datalist, property);
		UD.adddata(conf, datalist, property);
		System.out.println("是否加载协处理器(请输入yes or no)");
		String temp0=sc.nextLine();
		if(temp0.equals("no"))
		{
			return;
		}
		// 加载协处理器到表Coprocessor_Table
		HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
		System.out.println("正在加载协处理器……");
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName
				.valueOf("Coprocessor_Table"));
		System.out.println("tableDesc :" + tableDesc);
		if (!tableDesc
				.hasCoprocessor(OperatorLibraryEndpoint.class.getCanonicalName())) {
			System.out.println("有coprocessor");
			admin.disableTable(TableName.valueOf("Coprocessor_Table"));
			System.out.println("has disabled table " + "Coprocessor_Table");
			tableDesc.addCoprocessor(OperatorLibraryEndpoint.class.getCanonicalName(),
								new Path(path), Coprocessor.PRIORITY_USER, null);
			System.out.println("add coprocessor ok");
			admin.modifyTable(TableName.valueOf("Coprocessor_Table"), tableDesc);
			admin.enableTable(TableName.valueOf("Coprocessor_Table"));
			System.out.println("enable table ok");
		}
		System.out.println("是否进行计算(请输入yes or no)");
		String temp2=sc.nextLine();
		if(temp2.equals("no"))
		{
			return;
		}
		
		Main.Computer(property,conn);		
		String over=null;
		boolean over_bolean=true;
		while(over_bolean){
			System.out.println("是否结束算子计算，删除协处理器表(请输入yes or no)");
			over=sc.nextLine();
			if(!over.equals("yes")&&!over.equals("no"))
			{
				System.out.println("输入有误，请重新输入");continue;
			}	
			else if(over.equals("yes"))
			{
				UD.deletedata(conf, conn);
				over_bolean=false;
			}				
			else
			{
				Main.Computer(property,conn);
			}				
		}
	}
	//进行算子计算
	public static void Computer(String property,Connection conn) throws Exception{
		// 选择算子（判断输入算子是否正确：）
		Class<OperatorLibraryClient> cl = OperatorLibraryClient.class;
		Method[] mes = cl.getMethods();
		TreeMap<String, Method> methods = new TreeMap<String, Method>();
		for (int i = 0; i < mes.length - 9; i++) {
			methods.put(mes[i].getName(), mes[i]);
			methods.put(Integer.toString(i + 1), mes[i]);
			System.out.println(i + 1 + ":" + mes[i].getName());
		}
		Scanner sc = new Scanner(System.in);
		System.out.println("请输入算子编号或名称：");
		String Operator_name = null;
		boolean Operator_bool = true;
		while (Operator_bool) {
			Operator_name = sc.next();
			if (methods.containsKey(Operator_name))
				Operator_bool = false;
			else
				System.out.println("没有此算子，请重新输入");
		}
		Object ar[] = {property,conn};
		Object result_object = methods.get(Operator_name).invoke(
				OperatorLibraryClient.class.newInstance(), ar);
		if(methods.get(Operator_name).getReturnType()==List.class)
		{
			@SuppressWarnings("unchecked")
			List<Double> result = (List<Double>) result_object;
			
			if(result.contains(Double.MIN_VALUE))
			{
				System.out.println("输入有误(Startvalue or endvalue is error)");
			}
			else
			{
				System.out.println("结果为：");
				for (int i = 0; i < result.size(); i++) {
					System.out.println(result.get(i));
				}
			}		
		}else if(methods.get(Operator_name).getReturnType()==Double.class)
		{
			Double result = (Double) result_object;
			if(result==Double.MIN_VALUE)
			{
				System.out.println("输入有误(Startvalue or endvalue is error)");
			}
			else
			{
				System.out.println("结果为："+result);
			}
			
		}
	}
}
