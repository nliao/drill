package org.apache.drill;

import org.junit.Test;

/**
 * @author ningliao
 */
public class NingLiaoTest extends BaseTestQuery {

    @Test
    public void testOrphanGamma() throws Exception {
        testSql("select * from dfs.`/Library/WebServer/Documents/ov_gamma.csv` where columns[1] < 1048576");
    }

    @Test
    public void testOrphanGammaCount() throws Exception {
        testSql("select count(*) from dfs.`/Library/WebServer/Documents/ov_gamma.csv`");
    }

    @Test
    public void testOrphanGammaGroupBy() throws Exception {
        testSql("select columns[4] as bucket, count(*) from dfs.`/Library/WebServer/Documents/ov_gamma.csv` where bucket > 104 group by columns[4]");
    }

    @Test
    public void testOrphanSimple() throws Exception {
        testSql("select *, columns[0] from dfs.`/Library/WebServer/Documents/ov_stats.csv`");
    }

    @Test
    public void testOrphan() throws Exception {
        testSql("select columns[0], columns[4] from dfs.`/Library/WebServer/Documents/ov_stats.csv` where columns[1] >= 1048576");
    }

    @Test
    public void testCloudTrailJsonLines() throws Exception {
        String query = "SELECT t.eventName, t.userIdentity FROM  cp.`ningliao/data/cloudtrail_simple_array.json` t WHERE t.eventSource = 's3.amazonaws.com'";
        //String query = String.format("SELECT `eventName` FROM  cp.`%s` WHERE `eventSource` = 's3.amazonaws.com'", root);
        testSql(query);
    }

    @Test
    public void testCloudTrailJsonList() throws Exception {
        String query = "SELECT cast(t.eventName as int), t.userIdentity.accountId FROM cp.`ningliao/data/cloudtrail_simple_array2.json` t WHERE t.eventSource = 's3.amazonaws.com'";
        //String query = String.format("SELECT `eventName` FROM  cp.`%s` WHERE `eventSource` = 's3.amazonaws.com'", root);
        testSql(query);
    }

    /**
     * Must flatten array in order to access all the elements within it.
     * @throws Exception
     */
    @Test
    public void testCloudTrailJsonTuple() throws Exception {
        String root = "ningliao/data/cloudtrail_" +
                "simple.json";
        //String query = String.format("SELECT t FROM  (SELECT flatten(Records) AS record FROM dfs.`%s` ) t WHERE record.eventSource = 's3.amazonaws.com'", root);
        String query = String.format("SELECT t.record.eventName FROM  (SELECT flatten(Records) AS record FROM cp.`%s` ) t WHERE t.record.eventSource = 's3.amazonaws.com'", root);
        testSql(query);
    }

    @Test
    public void testTextInClasspathStorage_1() throws Exception {
        testSql("select columns[1] from cp.`/store/text/classpath_storage_csv_test.csv` where columns[0]=5");
    }

    @Test
    public void testJsonInClasspathStorage() throws Exception {
        testSql("select `integer` i, `float` f from cp.`jsoninput/input1.json` where `float` = '1.2'");
    }

    @Test
    public void testCloudTrailJson_Array() throws Exception {
        String root = "ningliao/data/cloudtrail_" +
                "simple_array2.json";
        String query = String.format("SELECT t.eventName, t.userIdentity FROM  cp.`%s` t WHERE t.eventSource = 's3.amazonaws.com'", root);
        //String query = String.format("SELECT `eventName` FROM  cp.`%s` WHERE `eventSource` = 's3.amazonaws.com'", root);
        testSql(query);
    }

    @Test
    public void testTestLogical_1() throws Exception {
        testLogicalFromFile("ningliao/plan/text_logical.json");
    }
}
