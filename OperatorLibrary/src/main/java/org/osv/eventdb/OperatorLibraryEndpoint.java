package org.osv.eventdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.special.Gamma;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryRequest;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryResponse;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryService;
 
/**
 * @author developer
 * 说明：hbase协处理器endpooint的服务端代码
 * 功能：继承通过protocol buffer生成的rpc接口，在服务端获取指定列的数据后进行求和操作，最后将结果返回客户端
 */
public class OperatorLibraryEndpoint extends OperatorLibraryService implements Coprocessor,CoprocessorService {
    
    private RegionCoprocessorEnvironment env;   // 定义环境
    
    @Override
    public Service getService() {
        return this;
    }
   
    // 协处理器初始化时调用的方法
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("no load region");
        }
    }
    
    // 协处理器结束时调用的方法
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        
    }

	@Override
	public void getGamma(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        List<Double> GammaResults = new ArrayList<>();
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {  
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;   
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                    double temp_result=Gamma.gamma(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                    GammaResults.add(temp_result);
                }  
                results.clear();  
            } while (hasMore);  
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addAllResult(GammaResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response);  
	}

	@Override
	public void getErf(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        List<Double> ErfResults = new ArrayList<>();
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {  
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;  
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                    double temp_result=Erf.erf(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                    ErfResults.add(temp_result);
                }  
                results.clear();  
            } while (hasMore);  
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addAllResult(ErfResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response);  
	}

	@Override
	public void getSum(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        double SumResults=0.0;
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {  
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;  
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                	SumResults=SumResults+Erf.erf(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                }  
                results.clear();  
            } while (hasMore);  
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addResult(SumResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response);  		
	}

	@Override
	public void getAve(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        double AveResults=0.0;
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {
        	double Sum=0.0;
        	int i=0;
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;  
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                	Sum=Sum+Erf.erf(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                	i++;
                }  
                results.clear();  
            } while (hasMore); 
            if(i==0)
            	AveResults=0.0;
            else
            	AveResults=Sum/i;
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addResult(AveResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response); 		
	}

	@Override
	public void getMax(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        double MaxResults=0.0;
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {
        	ArrayList<Double> alldata=new ArrayList<Double>();
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;  
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                	alldata.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                }  
                results.clear();  
            } while (hasMore); 
            MaxResults=Collections.max(alldata);
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addResult(MaxResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response); 		
	}

	@Override
	public void getMin(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
        // 定义变量  
        OperatorLibraryResponse response = null;  
        InternalScanner scanner = null;  
        double MinResults=0.0;
        // 设置扫描对象  
        Scan scan = new Scan();  
        scan.addFamily(Bytes.toBytes("data"));  
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(request.getColumn())); 
        // 扫描每个region，取值后求得对应结果  
        try {
        	ArrayList<Double> alldata=new ArrayList<Double>();
            scanner = env.getRegion().getScanner(scan);  
            List<Cell> results = new ArrayList<Cell>();  
            boolean hasMore = false;  
            do {  
                hasMore = scanner.next(results);  
                for (Cell cell : results) {  
                	alldata.add(Double.parseDouble(new String(CellUtil.cloneValue(cell))));
                }  
                results.clear();  
            } while (hasMore); 
            MinResults=Collections.min(alldata);
            // 设置返回结果  
    		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
    		responseBuilder.addResult(MinResults);
    		response = responseBuilder.build();
        } catch (IOException e) {  
            ResponseConverter.setControllerException(controller, e);  
        } finally {  
            if (scanner != null) {  
                try {  
                    scanner.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        } 
        // 将rpc结果返回给客户端  
        done.run(response); 	
	}
    

}

