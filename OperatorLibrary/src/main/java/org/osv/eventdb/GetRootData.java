package org.osv.eventdb;

import static net.blackruffy.root.JRoot.TTree;
import static net.blackruffy.root.JRoot.newTFile;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import net.blackruffy.root.TFile;
import net.blackruffy.root.TLeaf;
import net.blackruffy.root.TTree;
import net.sf.json.JSONObject;

public class GetRootData {
	
	public ArrayList<Double> getRootData(Configuration conf,String runID,String property,String startvalue,String stopvalue) throws IOException{
		ArrayList<Double> datalist = new ArrayList<Double>();
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		//连接请求表（判断输入是否正确）
		Scanner sc = new Scanner(System.in);
		System.out.println("请输入ROOT文件索引表表名：");
		String table_name = null;
		boolean table_bool = true;
		while (table_bool) {
			table_name = sc.nextLine();
					
			if(table_name.contains(" ")||table_name.matches("\\s+")||!admin.tableExists(TableName.valueOf(table_name)))
				System.out.println("没有此表，请重新输入表名：");		
			else
				table_bool = false;
		}
		HTable table = (HTable)conn.getTable(TableName.valueOf(table_name));
		
		//code(value)
		if(startvalue.contains("."))
			startvalue = getDoubleS(Double.parseDouble(startvalue));
		else
			startvalue = getIntS(Integer.parseInt(startvalue));
		if(stopvalue.contains("."))
			stopvalue = getDoubleS(Double.parseDouble(stopvalue));
		else
			stopvalue = getIntS(Integer.parseInt(stopvalue));
		//生成rowkey
		String startrowkey = runID+"#"+property+"#"+startvalue;
		String stoprowkey = runID+"#"+property+"#"+stopvalue;
		//1、开始查找索引表，进行表的扫描
		//2、从Hbase表中生成data文件的路径，存入path中
		//3、根据Hbase表中的偏移量和长度读取data文件中的内容，获得root文件的路径和偏移量，存入content中
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("data"));
		scan.setStartRow(Bytes.toBytes(startrowkey));
		scan.setStopRow(Bytes.toBytes(stoprowkey));
		ResultScanner scanner = table.getScanner(scan);
		ArrayList<String> content=new ArrayList<>();//用来存储从data文件中读到的内容
		
		for(Result result:scanner) {
			String[] str = new String[4];
			int i=0;
			for(Cell cell:result.rawCells())
			{
				str[i]=Bytes.toString(CellUtil.cloneValue(cell));
				i++;
			}
			String path = "hdfs://192.168.0.178:9000/eventdb/"+table_name+"/data/"+str[3]+".data";
			byte[] b=null;
			try {
				b = readFile(path,str[2],str[1]);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			content.add(new String(b));			
		}
		
		//1、对从data文件中读出来的内容进行JSON解析，获得ROOT文件的位置和偏移量
		//2、读取ROOT文件，获得data
		for(int i=0;i<content.size();i++)
		{
			try{
				JSONObject json = JSONObject.fromObject(content.get(i));
				@SuppressWarnings("rawtypes")
				Iterator json_iterator = json.keys();
				while(json_iterator.hasNext()) {
					String rootName = (String)json_iterator.next();										
					Object[] root_offset = json.getJSONArray(rootName).toArray();
					String rootPath="/home/shane/zyd/rootfile/0047543/"+rootName;
					final TFile file = newTFile(rootPath,"READ");															
					final TTree tree= TTree (file.get("ntTAG"));									
					TLeaf tleaf=tree.getLeaf(property);
					for(int j=0;j<root_offset.length;j++) {
						tree.getEntry((long) (int) root_offset[j]);	
						double data=tleaf.getValue();
						datalist.add(data);
					}
					tree.delete();
					file.close();				
					}
				}catch (Exception e) {
					datalist.add(Double.MIN_VALUE);
				}
		}
		return datalist;
	}
	
	//code(value)转换
  	public static String getDoubleS(double d) {
  		String s;
  		long b;
  		b = Double.doubleToLongBits(d);
  		b = (b^(b>>63 | 0x8000000000000000L)) + 1;
  		s = (Long.toHexString(b));
  		return s;
  	}
  	public static String getIntS(int d) {
  		d = d ^ 0x80000000;
  		String s = (Integer.toHexString(d));
  		return s;
  	}
  	/** 
     * 读取hdfs文件内容 
     * 
     * @param filePath 
	 * @return 
     * @throws IOException 
	 * @throws InterruptedException 
     */  
    public static byte[] readFile(String indexfilePath,String indexoffset,String indexlengh) throws IOException, InterruptedException {  
        Configuration conf1 = new Configuration(); 
        conf1.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Path srcPath = new Path(indexfilePath);  
        FileSystem fs = null;  
        URI uri;  
        byte[] b = new byte[Integer.parseInt(indexlengh)];
        InputStream in = null;
        try {  
            uri = new URI(indexfilePath);  
            fs = FileSystem.get(uri, conf1,"shane"); 
            if (!fs.exists(srcPath))
            {
            	b=Bytes.toBytes("no");
            }
            else
            {           	 
            	 in = fs.open(srcPath);
                 in.skip(Long.parseLong(indexoffset));
                 in.read(b);
                 in.close();
            }
        } catch (URISyntaxException e) {  
            e.printStackTrace();  
        } finally {  
            IOUtils.closeStream(in);  
        }
        return b;
    }

}
