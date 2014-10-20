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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

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
 **/
@InterfaceAudience.Private
public class TimeseriesAggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message>
    extends TimeseriesAggregateService implements CoprocessorService, Coprocessor {
  protected static final Log log = LogFactory.getLog(AggregateImplementation.class);
  private RegionCoprocessorEnvironment env;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // TODO Implement Coprocessor.stop

  }

  private TimeRange getInitialTimeRange(TimeseriesAggregateRequest request, Scan scan) {
    try {
      long interval = request.getTimeIntervalSeconds();
      if (!request.hasRange()) {
        return new TimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMin() + interval
            * 1000l);
      } else {
        long tsmin = request.getRange().getKeyTimestampMin();
        return new TimeRange(tsmin * 1000l, (long) (tsmin + interval) * 1000l);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private TimeRange getNextTimeRange(TimeRange timeRange, int interval) throws IOException {
    long intervalMillis = interval * 1000;
    return new TimeRange(timeRange.getMax(), timeRange.getMax() + intervalMillis);

  }

  private long getTimestampFromOffset(long currentTimeStamp, int offset) {
    long offsetMicro = (long) offset * 1000l;
    return currentTimeStamp + offsetMicro;
  }

  private long getSecondsTimestampFromRowKey(Cell kv, TimeseriesAggregateRequest request) {
    String keyPattern = request.getRange().getKeyTimestampFilterPattern();
    byte[] rowKey = CellUtil.cloneRow(kv);
    if (keyPattern.length() != rowKey.length) {
      log.error("Row Key:" + rowKey + ", Pattern: " + keyPattern);
      log.error("Timestamp Filter Pattern and Row Key length do not match. Don't know how to handle this.");
      return 0;
    }
    byte[] ts = new byte[4];
    int j = 0;
    for (int i = keyPattern.indexOf("1"); i <= keyPattern.lastIndexOf("1"); i++, j++) {
      ts[j] = rowKey[i];
    }
    return (long) Bytes.toInt(ts);
  }

  private long getMaxTimeStamp(Scan scan, TimeseriesAggregateRequest request) {
    if (request.hasRange()) {
      long timestamp = request.getRange().getKeyTimestampMax();
      return timestamp * 1000l;
    }
    return scan.getTimeRange().getMax();
  }

  @Override
  public void getMax(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    T max = null;
    boolean hasScannerRange = false;
    Map<Long, T> maximums = new HashMap<Long, T>();

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      byte[] qualifier = null;
      long maxTimeStamp = getMaxTimeStamp(scan, request);
      // qualifier can be null.
      boolean hasMoreRows = false;
      do {

        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(timestamp)) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              max = null;
            }
            if (intervalRange.withinTimeRange(timestamp)) {
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
              if (ci.compare(maximums.get(intervalRange), max) < 0) {
                maximums.put(intervalRange.getMin(), max);
              }
            }
          } else break;
        }
        results.clear();

      } while (hasMoreRows);
      if (!maximums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, T> entry : maximums.entrySet()) {
          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
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

  @SuppressWarnings("unchecked")
  ColumnInterpreter<T, S, P, Q, R> constructColumnInterpreterFromRequest(
      TimeseriesAggregateRequest request) throws IOException {
    String className = request.getInterpreterClassName();
    Class<?> cls;
    try {
      cls = Class.forName(className);
      ColumnInterpreter<T, S, P, Q, R> ci = (ColumnInterpreter<T, S, P, Q, R>) cls.newInstance();
      if (request.hasInterpreterSpecificBytes()) {
        ByteString b = request.getInterpreterSpecificBytes();
        P initMsg = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 2, b);
        ci.initialize(initMsg);
      }
      return ci;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void getMin(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    InternalScanner scanner = null;
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    T min = null;
    boolean hasScannerRange = false;
    Map<Long, T> minimums = new HashMap<Long, T>();

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      byte[] qualifier = null;
      long maxTimeStamp = getMaxTimeStamp(scan, request);

      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(timestamp)) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              min = null;
            }
            if (intervalRange.withinTimeRange(timestamp)) {
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              min = (min == null || (temp != null && ci.compare(temp, min) < 0)) ? temp : min;
              if (ci.compare(min, minimums.get(intervalRange)) > 0) {
                minimums.put(intervalRange.getMin(), min);
              }
            }
          } else break;
        }
        results.clear();
      } while (hasMoreRows);
      if (minimums != null) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, T> entry : minimums.entrySet()) {
          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());

          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
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
    Map<Long, S> sums = new HashMap<Long, S>();
    long sum = 0l;
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      long maxTimeStamp = getMaxTimeStamp(scan, request);
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(timestamp)) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              sumVal = null;
            }
            if (intervalRange.withinTimeRange(timestamp)) {
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              if (temp != null) sumVal = ci.add(sumVal, ci.castToReturnType(temp));
              sums.put(intervalRange.getMin(), sumVal);
            }
          } else break;
        }
        results.clear();
      } while (hasMoreRows);
      if (!sums.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, S> entry : sums.entrySet()) {

          TimeseriesAggregateResponseEntry responseElement = null;
          TimeseriesAggregateResponseMapEntry mapEntry = null;
          TimeseriesAggregateResponseEntry.Builder valueBuilder =
              TimeseriesAggregateResponseEntry.newBuilder();
          TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          valueBuilder.addFirstPart(ci.getProtoForPromotedType(entry.getValue()).toByteString());

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
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
    Map<Long, SimpleEntry<Long, S>> averages = new HashMap<Long, SimpleEntry<Long, S>>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }

    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Long kvCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      byte[] qualifier = null;
      long maxTimeStamp = getMaxTimeStamp(scan, request);

      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;

      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(timestamp)) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              kvCountVal = 0l;
              sumVal = null;
            }
            if (intervalRange.withinTimeRange(timestamp)) {
              kvCountVal++;
              temp = ci.getValue(colFamily, kv.getQualifier(), kv);
              if (temp != null) sumVal = ci.add(sumVal, ci.castToReturnType(temp));
              averages.put(intervalRange.getMin(), new AbstractMap.SimpleEntry<Long, S>(kvCountVal,
                  sumVal));
            }
          } else break;
        }
      } while (hasMoreRows);
      if (!averages.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Entry<Long, SimpleEntry<Long, S>> entry : averages.entrySet()) {
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

          mapElementBuilder.setKey(entry.getKey());
          mapElementBuilder.setValue(valueBuilder.build());
          responseBuilder.addEntry(mapElementBuilder.build());
        }
        response = responseBuilder.build();
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
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    InternalScanner scanner = null;
    Map<TimeRange, SimpleEntry<Long, Pair<S, S>>> stds =
        new HashMap<TimeRange, SimpleEntry<Long, Pair<S, S>>>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null, sumSqVal = null, tempVal = null;
      long kvCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      long maxTimeStamp = getMaxTimeStamp(scan, request);
      List<Cell> results = new ArrayList<Cell>();

      boolean hasMoreRows = false;

      do {
        tempVal = null;
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(kv.getTimestamp())) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              kvCountVal = 0l;
              sumVal = null;
              sumSqVal = null;
              tempVal = null;
            }
            if (intervalRange.withinTimeRange(timestamp)) {
              kvCountVal++;
              tempVal =
                  ci.add(tempVal,
                    ci.castToReturnType(ci.getValue(colFamily, kv.getQualifier(), kv)));
              sumVal = ci.add(sumVal, tempVal);
              sumSqVal = ci.add(sumSqVal, ci.multiply(tempVal, tempVal));
              stds.put(new TimeRange(intervalRange.getMin(), intervalRange.getMax()),
                new AbstractMap.SimpleEntry<Long, Pair<S, S>>(kvCountVal, new Pair<S, S>(sumVal,
                    sumSqVal)));
            }
          } else break;
        }
      } while (hasMoreRows);
      if (!stds.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Entry<TimeRange, SimpleEntry<Long, Pair<S, S>>> entry : stds.entrySet()) {
          TimeseriesAggregateResponseMapEntry.Builder mapEntryBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          ByteString first_sumVal =
              ci.getProtoForPromotedType(entry.getValue().getValue().getFirst()).toByteString();
          ByteString first_sumSqVal =
              ci.getProtoForPromotedType(entry.getValue().getValue().getSecond()).toByteString();
          TimeseriesAggregateResponseEntry.Builder responsePair =
              TimeseriesAggregateResponseEntry.newBuilder();
          responsePair.addFirstPart(first_sumVal);
          responsePair.addFirstPart(first_sumSqVal);
          ByteBuffer bb = ByteBuffer.allocate(8).putLong(entry.getValue().getKey());
          bb.rewind();
          responsePair.setSecondPart(ByteString.copyFrom(bb));

          mapEntryBuilder.setKey(entry.getKey().getMin());
          mapEntryBuilder.setValue(responsePair.build());
          responseBuilder.addEntry(mapEntryBuilder.build());
        }
        response = responseBuilder.build();
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
  public void getMedian(RpcController controller, TimeseriesAggregateRequest request,
      RpcCallback<TimeseriesAggregateResponse> done) {
    TimeseriesAggregateResponse response = null;
    TimeRange intervalRange = null;
    InternalScanner scanner = null;
    Map<TimeRange, Pair<S, S>> medians = new HashMap<TimeRange, Pair<S, S>>();
    boolean hasScannerRange = false;

    if (!request.hasRange()) {
      hasScannerRange = true; // When no timerange is being passed in via
      // the request, it is
      // assumed, that the scanner is
      // timestamp-range bound
    }
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null, sumWeights = null, tempVal = null, tempWeight = null;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      intervalRange = getInitialTimeRange(request, scan);
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] valQualifier = null, weightQualifier = null;
      long maxTimeStamp = getMaxTimeStamp(scan, request);
      if (qualifiers != null && !qualifiers.isEmpty()) {
        valQualifier = qualifiers.pollFirst();
        // if weighted median is requested, get qualifier for the weight
        // column
        weightQualifier = qualifiers.pollLast();
      }
      List<Cell> results = new ArrayList<Cell>();

      boolean hasMoreRows = false;

      do {
        tempVal = null;
        tempWeight = null;
        hasMoreRows = scanner.next(results);
        for (Cell kv : results) {
          if (intervalRange.getMin() < maxTimeStamp) {
            long timestamp = 0;
            if (hasScannerRange) timestamp = kv.getTimestamp();
            else timestamp =
                getTimestampFromOffset(getSecondsTimestampFromRowKey(kv, request) * 1000l,
                  Bytes.toInt(kv.getQualifier()));
            if (!intervalRange.withinTimeRange(timestamp)) {
              intervalRange = getNextTimeRange(intervalRange, request.getTimeIntervalSeconds());
              tempVal = null;
              tempWeight = null;
              sumVal = null;
              sumWeights = null;
            } else if (intervalRange.withinTimeRange(timestamp)) {
              tempVal =
                  ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily, valQualifier, kv)));
              if (weightQualifier != null) {
                tempWeight =
                    ci.add(tempWeight,
                      ci.castToReturnType(ci.getValue(colFamily, weightQualifier, kv)));
              }
              sumVal = ci.add(sumVal, tempVal);
              sumWeights = ci.add(sumWeights, tempWeight);
              if (tempWeight != null) medians.put(new TimeRange(intervalRange.getMin(),
                  intervalRange.getMax()), new Pair<S, S>(sumVal, tempVal));
            }
          } else break;
        }
      } while (hasMoreRows);
      if (!medians.isEmpty()) {
        TimeseriesAggregateResponse.Builder responseBuilder =
            TimeseriesAggregateResponse.newBuilder();

        for (Entry<TimeRange, Pair<S, S>> entry : medians.entrySet()) {
          TimeseriesAggregateResponseMapEntry.Builder mapEntryBuilder =
              TimeseriesAggregateResponseMapEntry.newBuilder();

          ByteString first_sumVal =
              ci.getProtoForPromotedType(entry.getValue().getFirst()).toByteString();
          S s =
              entry.getValue().getSecond() == null ? ci.castToReturnType(ci.getMinValue()) : entry
                  .getValue().getSecond();
          ByteString first_sumWeights = ci.getProtoForPromotedType(s).toByteString();
          TimeseriesAggregateResponseEntry.Builder responsePair =
              TimeseriesAggregateResponseEntry.newBuilder();
          responsePair.addFirstPart(first_sumVal);
          responsePair.addFirstPart(first_sumWeights);

          mapEntryBuilder.setKey(entry.getKey().getMin());
          mapEntryBuilder.setValue(responsePair.build());
          responseBuilder.addEntry(mapEntryBuilder.build());
        }
        response = responseBuilder.build();
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
}