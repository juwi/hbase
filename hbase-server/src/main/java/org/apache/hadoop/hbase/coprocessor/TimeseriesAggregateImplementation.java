package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete TimeseriesAggregateProtocol implementation. Its system level coprocessor
 * that computes the aggregate function at a region level.
 * {@link ColumnInterpreter} is used to interpret column value. This class is
 * parameterized with the following (these are the types with which the {@link ColumnInterpreter}
 * is parameterized, and for more description on these, refer to {@link ColumnInterpreter}):
 * @param <T> Cell value data type
 * @param <S> Promoted data type
 * @param <P> PB message that is used to transport initializer specific bytes
 * @param <Q> PB message that is used to transport Cell (<T>) instance
 * @param <R> PB message that is used to transport Promoted (<S>) instance
 */
@InterfaceAudience.Private
public class TimeseriesAggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message> 
extends TimeseriesAggregateService implements CoprocessorService, Coprocessor {
  protected static final Log log = LogFactory.getLog(AggregateImplementation.class);
  private RegionCoprocessorEnvironment env;
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    // TODO Implement Coprocessor.start
    
  }
  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // TODO Implement Coprocessor.stop
    
  }
  @Override
  public Service getService() {
    // TODO Implement CoprocessorService.getService
    return null;
  }
  @Override
  public void getMax(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getMax
    
  }
  @Override
  public void getMin(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getMin
    
  }
  @Override
  public void getSum(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getSum
    
  }
  @Override
  public void getRowNum(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getRowNum
    
  }
  @Override
  public void getAvg(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getAvg
    
  }
  @Override
  public void getStd(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getStd
    
  }
  @Override
  public void getMedian(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getMedian
    
  }

}
