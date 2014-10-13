package org.apache.hadoop.hbase.coprocessor;

import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestTimeseriesAggregateProtocol {
  protected static Log myLog = LogFactory.getLog(TestTimeseriesAggregateProtocol.class);

  
  /**
   * Creating the test infrastructure.
   */
  private static final TableName TEST_TABLE =
      TableName.valueOf("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_MULTI_CQ = Bytes.toBytes("TestMultiCQ");

  private static String ROW = "testRow";
  private static int TIME_BASELINE = (int)(new GregorianCalendar(2014,10,10,0,0,0).getTime().getTime()/1000);
  private static final int ROWSIZE = 100;
  private static final int rowSeperator1 = 25;
  private static final int rowSeperator2 = 60;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE, TIME_BASELINE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();

  /**
   * A set up method to start the test cluster. AggregateProtocolImpl is
   * registered and will be loaded during region startup.
   * @throws Exception
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.coprocessor.TimeseriesAggregateImplementation");

    util.startMiniCluster(2);
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
//    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
//        new byte[][] { HConstants.EMPTY_BYTE_ARRAY, ROWS[rowSeperator1],
//            ROWS[rowSeperator2] });
    /**
     * The testtable has one CQ which is always populated and one variable CQ
     * for each row rowkey1: CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
//    for (int i = 0; i < ROWSIZE; i++) {
//      Put put = new Put(ROWS[i]);
//      put.setDurability(Durability.SKIP_WAL);
//      Long l = new Long(i);
//      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(l));
//      table.put(put);
//      Put p2 = new Put(ROWS[i]);
//      put.setDurability(Durability.SKIP_WAL);
//      p2.add(TEST_FAMILY, Bytes.add(TEST_MULTI_CQ, Bytes.toBytes(l)), Bytes
//          .toBytes(l * 10));
//      table.put(p2);
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
  private static byte[][] makeN(String base, int n, int time) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      
      ret[i] = Bytes.add((base + time).getBytes(), Bytes.toBytes(i));
      time += 3600;
    }
    return ret;
  }
}
