package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.TimeseriesAggregationClient;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestTimeseriesAggregateProtocol {
  protected static Log log = LogFactory.getLog(TestTimeseriesAggregateProtocol.class);

  /**
   * Creating the test infrastructure.
   */
  private static final TableName TEST_TABLE = TableName.valueOf("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final String KEY_FILTER_PATTERN = "00000001111";
  private static String ROW = "testRow";
  private static int TIME_BASELINE = (int) ((new GregorianCalendar(2014, 10, 10, 0, 0, 0).getTime()
      .getTime()) / 1000);
  private static final byte[] START_ROW = Bytes.add(ROW.getBytes(), Bytes.toBytes(TIME_BASELINE));
  private static final byte[] STOP_ROW = Bytes.add(ROW.getBytes(),
    Bytes.toBytes(TIME_BASELINE + (3600 * 2)));
  private static final int ROWSIZE = 100;
  private static final int rowSeperator1 = 25;
  private static final int rowSeperator2 = 60;
  private static List<Pair<byte[], Map<byte[], byte[]>>> ROWS = makeN(ROW, ROWSIZE, TIME_BASELINE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();

  /**
   * A set up method to start the test cluster. AggregateProtocolImpl is registered and will be
   * loaded during region startup.
   * @throws Exception
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.coprocessor.TimeseriesAggregateImplementation");

    util.startMiniCluster(2);
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY, new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY, ROWS.get(rowSeperator1).getFirst(),
        ROWS.get(rowSeperator2).getFirst() });
    /**
     * The testtable has one CQ which is always populated and one variable CQ for each row rowkey1:
     * CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS.get(i).getFirst());
      for (Map.Entry<byte[], byte[]> entry : ROWS.get(i).getSecond().entrySet()) {
        put.add(TEST_FAMILY, entry.getKey(), entry.getValue());
      }
      put.setDurability(Durability.SKIP_WAL);
      table.put(put);
    }
    table.close();
  }

  /**
   * Shutting down the cluster
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * an infrastructure method to prepare rows for the testtable.
   * @param base
   * @param n
   * @return
   */
  private static List<Pair<byte[], Map<byte[], byte[]>>> makeN(String base, int n, int time) {
    List<Pair<byte[], Map<byte[], byte[]>>> ret =
        new ArrayList<Pair<byte[], Map<byte[], byte[]>>>();
    int interval = 3600 / n;

    for (int i = 0; i < n; i++) {
      Map<byte[], byte[]> innerMap = new LinkedHashMap<byte[], byte[]>();
      byte[] key = Bytes.add(base.getBytes(), Bytes.toBytes(time));
      int cq = 0;
      for (int j = 0; j < n; j++) {
        if (j != 0) cq += interval;
        long value = j;
        innerMap.put(Bytes.toBytes(cq), Bytes.toBytes(value));
      }
      ret.add(new Pair<byte[], Map<byte[], byte[]>>(key, innerMap));
      time += 3600;
    }
    return ret;
  }

  /**
   * ****************** Test cases for Median **********************
   */
  /**
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMedianWithValidRange() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    // scan.setStartRow(START_ROW);
    // scan.setStopRow(STOP_ROW);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    ConcurrentSkipListMap<Long, Long> median = aClient.median(TEST_TABLE, ci, scan);
    assertEquals(49L, median);
  }

  /**
   * ***************Test cases for Maximum *******************
   */

  /**
   * give max for the entire table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMaxWithValidRange() throws Throwable {
    HTable t = new HTable(conf, TEST_TABLE);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    // scan.setStartRow(START_ROW);
    // scan.setStopRow(STOP_ROW);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(99, maximum);
  }

}
