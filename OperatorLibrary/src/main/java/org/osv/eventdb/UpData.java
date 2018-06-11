package org.osv.eventdb;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UpData {
	public void adddata(Configuration conf,Connection conn,ArrayList<Double> datalist,String property) throws IOException{
		HBaseAdmin HbaseAdmin = new HBaseAdmin(conn);
		//定义协处理器的表名，创建存储数据、能进行协处理器计算的表
		String tableName="Coprocessor_Table";
		if (!HbaseAdmin.tableExists(tableName)) {// 表不存在的时候，再创建  
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("data"));
			HbaseAdmin.createTable(tableDescriptor);
        }
		//添加数据
        //创建表连接    
        HTable table=new HTable(conf,tableName.valueOf(tableName));    
        //将数据自动提交功能关闭    
        table.setAutoFlush(false);    
        //设置数据缓存区域    
        table.setWriteBufferSize(128*1024); 
        //然后开始写入数据 
        for(int i=0;i<datalist.size();i++)
        {
        	Put put=new Put(Bytes.toBytes("row"+i));  //建立行建的put对象
        	put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(property), Bytes.toBytes(String.valueOf(datalist.get(i))));
        	table.put(put);
        }
        //刷新缓存区
        table.flushCommits(); 
        //关闭表连接    
        table.close(); 
        System.out.println("建表成功");
	}
	//删除数据
	public void deletedata(Configuration conf,Connection conn) throws IOException{
		
		String tableName="Coprocessor_Table";
        HBaseAdmin Hbaseadmin = new HBaseAdmin(conn);
        Hbaseadmin.disableTable(tableName);
        Hbaseadmin.deleteTable(tableName);
        Hbaseadmin.close();
        System.out.println("删表成功");
	}
}
