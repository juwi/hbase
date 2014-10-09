package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;


/**
 * This client class is for invoking the aggregate functions deployed on the
 * Region Server side via the TimeseriesAggregateService. This class will implement the
 * supporting functionality for summing/processing the individual results
 * obtained from the TimeseriesAggregateService for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate
 * functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the
 * parameter type. For average and std, it returns a double value. For row
 * count, it returns a long value.
 * 
 * <T> Cell value data type
 * <S> Promoted data type
 * <P> PB message that is used to transport initializer specific bytes
 * <Q> PB message that is used to transport Cell (<T>) instance
 * <R> PB message that is used to transport Promoted (<S>) instance
 */
@InterfaceAudience.Private
public class TimeseriesAggregationClient {

  private static final Log log = LogFactory.getLog(TimeseriesAggregationClient.class);
  Configuration conf;

  /**
   * Constructor with Conf object
   * @param cfg
   */
  public TimeseriesAggregationClient(Configuration cfg) {
    this.conf = cfg;
  }
  
  /**
   * It gives the maximum value of a column for a given column family for the
   * given range. In case qualifier is null, a max of all values for the given
   * family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return max val <R> (Will come as proto from region needs to be passed out as ConcurrentSkipListMap)
   * @throws Throwable
   *           The caller is supposed to handle the exception as they are thrown
   *           & propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> ConcurrentSkipListMap max(
      final TableName tableName, final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan)
      throws Throwable {
    Table table = null;
    try {
      table = new HTable(conf, tableName);
      return max(table, ci, scan);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * It gives the maximum value of a column for a given column family for the
   * given range. In case qualifier is null, a max of all values for the given
   * family is returned.
   * @param table
   * @param ci
   * @param scan
   * @return max val <R> (Will come as proto from region needs to be passed out as ConcurrentSkipListMap)
   * @throws Throwable
   *           The caller is supposed to handle the exception as they are thrown
   *           & propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message> 
  ConcurrentSkipListMap max(final Table table, final ColumnInterpreter<R, S, P, Q, T> ci,
      final Scan scan) throws Throwable {
    final TimeseriesAggregateRequest requestArg = validateArgAndGetPB(scan, ci, false);
    class MaxCallBack implements Batch.Callback<R> {
      R max = null;

      R getMax() {
        return max;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        max = (max == null || (result != null && ci.compare(max, result) < 0)) ? result : max;
      }
    }
    MaxCallBack aMaxCallBack = new MaxCallBack();
    table.coprocessorService(TimeseriesAggregateService.class, scan.getStartRow(), scan.getStopRow(),
        new Batch.Call<TimeseriesAggregateService, R>() {
          @Override
          public R call(TimeseriesAggregateService instance) throws IOException {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<TimeseriesAggregateResponse> rpcCallback = 
                new BlockingRpcCallback<TimeseriesAggregateResponse>();
            instance.getMax(controller, requestArg, rpcCallback);
            TimeseriesAggregateResponse response = rpcCallback.get();
            if (controller.failedOnException()) {
              throw controller.getFailedOn();
            }
            if (response.getFirstPartCount() > 0) {
              ByteString b = response.getFirstPart(0);
              Q q = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 3, b);
              return ci.getCellValueFromProto(q);
            }
            return null;
          }
        }, aMaxCallBack);
    return aMaxCallBack.getMax();
  }

  /*
   * @param scan
   * @param canFamilyBeAbsent whether column family can be absent in familyMap of scan
   */
  private void validateParameters(Scan scan, boolean canFamilyBeAbsent) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
            .equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) &&
          !Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      throw new IOException(
          "Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (!canFamilyBeAbsent) {
      if (scan.getFamilyMap().size() != 1) {
        throw new IOException("There must be only one family.");
      }
    }
  }
  
  <R, S, P extends Message, Q extends Message, T extends Message> TimeseriesAggregateRequest 
  validateArgAndGetPB(Scan scan, ColumnInterpreter<R,S,P,Q,T> ci, boolean canFamilyBeAbsent)
      throws IOException {
    validateParameters(scan, canFamilyBeAbsent);
    final TimeseriesAggregateRequest.Builder requestBuilder = 
        TimeseriesAggregateRequest.newBuilder();
    requestBuilder.setInterpreterClassName(ci.getClass().getCanonicalName());
    P columnInterpreterSpecificData = null;
    if ((columnInterpreterSpecificData = ci.getRequestData()) 
       != null) {
      requestBuilder.setInterpreterSpecificBytes(columnInterpreterSpecificData.toByteString());
    }
    requestBuilder.setScan(ProtobufUtil.toScan(scan));
    return requestBuilder.build();
  }


}
