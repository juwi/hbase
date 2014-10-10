package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete TimeseriesAggregateProtocol implementation. Its system level coprocessor that computes
 * the aggregate function at a region level. {@link ColumnInterpreter} is used to interpret column
 * value. This class is parameterized with the following (these are the types with which the
 * {@link ColumnInterpreter} is parameterized, and for more description on these, refer to
 * {@link ColumnInterpreter}):
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
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    T max = null;
    boolean hasScannerRange = false;
    Map<TimeRange, T> maximums = new HashMap<TimeRange, T>();

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via the request, it is
                              // assumed, that the scanner is timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      if (hasScannerRange) {
        intervalRange =
            new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin()
                + request.getTimeIntervalSeconds() * 1000);
      } else {
        intervalRange =
            new TimeRange(request.getRange().getKeyTimestampMin() * 1000, (request.getRange()
                .getKeyTimestampMin() + request.getTimeIntervalSeconds()) * 1000);
      }
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      // qualifier can be null.
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              Bytes.toLong(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()) * 1000;
          if (!intervalRange.withinTimeRange(timestamp)) {
            intervalRange =
                new TimeRange(intervalRange.getMax(), intervalRange.getMax()
                    + request.getTimeIntervalSeconds());
          }
          if (intervalRange.withinTimeRange(timestamp)) {
            temp = ci.getValue(colFamily, qualifier, kv);
            max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
            if (ci.compare(maximums.get(intervalRange), max) < 0) {
              maximums.put(new TimeRange(intervalRange.getMin(), intervalRange.getMax()), max);
            }
          }
        }
        results.clear();
      } while (hasMoreRows);
      if (!maximums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<TimeRange, T> entry : maximums.entrySet()) {
          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setIntervalStart(entry.getKey().getMin());
          mapElementBuilder.setIntervalEnd(entry.getKey().getMax());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Maximum from this region is " + env.getRegion().getRegionNameAsString() + ": " + max);
    done.run(response);
  }

  private ColumnInterpreter<T, S, P, Q, R> constructColumnInterpreterFromRequest(
      TimeseriesAggregateRequest request) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void getMin(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    T min = null;
    boolean hasScannerRange = false;
    Map<TimeRange, T> minimums = new HashMap<TimeRange, T>();

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via the request, it is
                              // assumed, that the scanner is timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      if (hasScannerRange) {
        intervalRange =
            new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin()
                + request.getTimeIntervalSeconds() * 1000);
      } else {
        intervalRange =
            new TimeRange(request.getRange().getKeyTimestampMin() * 1000, (request.getRange()
                .getKeyTimestampMin() + request.getTimeIntervalSeconds()) * 1000);
      }
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              Bytes.toLong(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()) * 1000;
          if (!intervalRange.withinTimeRange(timestamp)) {
            intervalRange =
                new TimeRange(intervalRange.getMax(), intervalRange.getMax()
                    + request.getTimeIntervalSeconds());
          }
          if (intervalRange.withinTimeRange(timestamp)) {
            temp = ci.getValue(colFamily, qualifier, kv);
            min = (min == null || (temp != null && ci.compare(temp, min) < 0)) ? temp : min;
            if (ci.compare(minimums.get(intervalRange), min) > 0) {
              minimums.put(new TimeRange(intervalRange.getMin(), intervalRange.getMax()), min);
            }
          }
        }
        results.clear();
      } while (hasMoreRows);
      if (minimums != null) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<TimeRange, T> entry : minimums.entrySet()) {
          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setIntervalStart(entry.getKey().getMin());
          mapElementBuilder.setIntervalEnd(entry.getKey().getMax());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.info("Minimum from this region is " + env.getRegion().getRegionNameAsString() + ": " + min);
    done.run(response);

  }

  @Override
  public void getSum(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    InternalScanner scanner = null;
    Map<TimeRange, S> sums = new HashMap<TimeRange, S>();
    long sum = 0l;
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via the request, it is
                              // assumed, that the scanner is timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      if (hasScannerRange) {
        intervalRange =
            new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin()
                + request.getTimeIntervalSeconds() * 1000);
      } else {
        intervalRange =
            new TimeRange(request.getRange().getKeyTimestampMin() * 1000, (request.getRange()
                .getKeyTimestampMin() + request.getTimeIntervalSeconds()) * 1000);
      }
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              Bytes.toLong(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()) * 1000;
          if (!intervalRange.withinTimeRange(timestamp)) {
            intervalRange =
                new TimeRange(intervalRange.getMax(), intervalRange.getMax()
                    + request.getTimeIntervalSeconds());
            sumVal = null;
          }
          if (intervalRange.withinTimeRange(timestamp)) {
            temp = ci.getValue(colFamily, qualifier, kv);
            if (temp != null) sumVal = ci.add(sumVal, ci.castToReturnType(temp));
            sums.put(new TimeRange(intervalRange.getMin(), intervalRange.getMax()), sumVal);
          }
        }
        results.clear();
      } while (hasMoreRows);
      if (!sums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<TimeRange, S> entry : sums.entrySet()) {

          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForPromotedType(entry.getValue()).toByteString());

          mapElementBuilder.setIntervalStart(entry.getKey().getMin());
          mapElementBuilder.setIntervalEnd(entry.getKey().getMax());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    log.debug("Sum from this region is " + env.getRegion().getRegionNameAsString() + ": " + sum);
    done.run(response);
  }

  @Override
  public void getRowNum(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    // TODO Implement TimeseriesAggregateService.getRowNum

  }

  @Override
  public void getAvg(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    InternalScanner scanner = null;
    Map<TimeRange, SimpleEntry<Long, S>> averages = new HashMap<TimeRange, SimpleEntry<Long, S>>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via the request, it is
                              // assumed, that the scanner is timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Long kvCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      if (hasScannerRange) {
        intervalRange =
            new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin()
                + request.getTimeIntervalSeconds() * 1000);
      } else {
        intervalRange =
            new TimeRange(request.getRange().getKeyTimestampMin() * 1000, (request.getRange()
                .getKeyTimestampMin() + request.getTimeIntervalSeconds()) * 1000);
      }
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;

      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          long timestamp = 0;
          if (hasScannerRange) timestamp = kv.getTimestamp();
          else timestamp =
              Bytes.toLong(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()) * 1000;
          if (!intervalRange.withinTimeRange(timestamp)) {
            intervalRange =
                new TimeRange(intervalRange.getMax(), intervalRange.getMax()
                    + request.getTimeIntervalSeconds());
            kvCountVal = 0l;
            sumVal = null;
          }
          if (intervalRange.withinTimeRange(timestamp)) {
            kvCountVal++;
            temp = ci.getValue(colFamily, qualifier, kv);
            if (temp != null) sumVal = ci.add(sumVal, ci.castToReturnType(temp));
            averages.put(new TimeRange(intervalRange.getMin(), intervalRange.getMax()),
              new AbstractMap.SimpleEntry<Long, S>(kvCountVal, sumVal));
          }
        }
      } while (hasMoreRows);
      if (!averages.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Entry<TimeRange, SimpleEntry<Long, S>> entry : averages.entrySet()) {
          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          ByteString first = ci.getProtoForPromotedType(entry.getValue().getValue()).toByteString();
          valueBuilder.addFirstPart(first);
          ByteBuffer bb = ByteBuffer.allocate(8).putLong(entry.getValue().getKey());
          bb.rewind();
          valueBuilder.setSecondPart(ByteString.copyFrom(bb));

          mapElementBuilder.setIntervalStart(entry.getKey().getMin());
          mapElementBuilder.setIntervalEnd(entry.getKey().getMax());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }
    done.run(response);
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
