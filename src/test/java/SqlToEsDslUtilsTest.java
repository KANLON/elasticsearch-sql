import com.kanlon.utils.SQLToEsDSLUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * sql 转为es的dsl语法测试
 *
 * @author zhangcanlong
 * @since 2021/5/10 1:15
 **/
public class SqlToEsDslUtilsTest {
    /**
     * 测试 输出
     */
    @Test
    public void test() {
        System.out.println("");
    }

    /**
     * 测试sql es dsl
     */
    @Test
    public void testSqlToEsDsl() {
        String sql1 = "select appid,uid,`dt`,dt_time,time from    default.test_t1 where dt_time>='202105081313+0800' and dt_time <='202105081413+0800' and appid = '15013' and uid in ('2524712316','2706555022')  order by dt_time desc,time desc limit 0,5";
        String sql2 = "select count(1) from    default.test_t2 where dt='12' and country like '%张三%'";
        String sql3 = "select * from    default.test_t3 where dt='asdf' limit 10 ";
        String sql4 = "select count(1) from ( select * from    default.test_t4 where dt='asdf' limit 10 union all  select * from    default.test_t3 where dt='asdf' limit 10  union all  select * from    default.test_t4 where dt='asdf' limit 10 )t ";
        String sql5 = "select * from    default.test_t5 where dt='adsf' union all  select * from    default.test12 where dt='12'  union all  select * from    default.test_t7 where dt='12' order by desc  limit 10 ";
        String sql6 = "SELECT * FROM `default.test_t6` WHERE dt_time >= '202105091817+0800'  AND dt_time <= '202105111817+0800'  AND country LIKE '%中国黑龙江绥化%' UNION ALL SELECT * FROM `default.test_t7` WHERE dt_time >= '202105091817+0800'  AND dt_time <= '202105111817+0800'  AND country LIKE '%中国黑龙江绥化%' UNION ALL SELECT * FROM `default.test_t123` WHERE dt_time >= '202105091817+0800'  AND dt_time <= '202105111817+0800'  AND country LIKE '%中国黑龙江绥化%' LIMIT 10";
        String sql7 = "SELECT count(1) AS num FROM (  SELECT *  FROM `default.test_t7`  WHERE dt_time >= '202105092057+0800'   AND dt_time <= '202105112057+0800'   AND act = '7609'  UNION ALL  SELECT *  FROM `default.test_t8`  WHERE dt_time >= '202105092057+0800'   AND dt_time <= '202105112057+0800'   AND act = '7609'  UNION ALL  SELECT *  FROM `default.test_t11,`  WHERE dt_time >= '202105092057+0800'   AND dt_time <= '202105112057+0800'   AND act = '7609'  LIMIT 2147483647 ) temp_t ";
        String sql8 = "SELECT * FROM `default.test_t8` WHERE dt_time >= '202105092057+0800'  AND dt_time <= '202105112057+0800'  AND act = '7609' UNION ALL SELECT * FROM `default.test_t11` WHERE dt_time >= '202105092057+0800'  AND dt_time <= '202105112057+0800'  AND act = '7609' UNION ALL SELECT * FROM `default.test_t13` WHERE dt_time >= '202105092057+0800'  AND dt_time <= '202105112057+0800'  AND act = '7609' LIMIT 10";
        try {
            // 下面得到的实例信息，toString 之后，是原生es的 DSL 语法，将该json 作为body可以直接通过ES 的RESTful API查询es
            String expRet1 = "{\"from\":0,\"size\":5,\"query\":{\"bool\":{\"must\":[{\"range\":{\"dt_time\":{\"from\":\"202105081313+0800\",\"to\":null,\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"range\":{\"dt_time\":{\"from\":null,\"to\":\"202105081413+0800\",\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"term\":{\"appid\":{\"value\":\"15013\",\"boost\":1.0}}},{\"terms\":{\"uid\":[\"2524712316\",\"2706555022\"],\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":{\"includes\":[\"dt\",\"uid\",\"appid\",\"time\",\"dt_time\"],\"excludes\":[]},\"sort\":[{\"dt_time\":{\"order\":\"desc\"}},{\"time\":{\"order\":\"desc\"}}]}";
            String actRet1 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql1).toString();
            Assert.assertEquals(expRet1, actRet1);

            String expRet2 = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"dt\":{\"value\":\"12\",\"boost\":1.0}}},{\"wildcard\":{\"country\":{\"wildcard\":\"*张三*\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet2 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql2).toString();
            Assert.assertEquals(expRet2, actRet2);

            String expRet3 = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":[{\"term\":{\"dt\":{\"value\":\"asdf\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet3 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql3).toString();
            Assert.assertEquals(expRet3, actRet3);

            String expRet4 = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"dt\":{\"value\":\"asdf\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet4 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql4).toString();
            Assert.assertEquals(expRet4, actRet4);

            String expRet5 = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":[{\"term\":{\"dt\":{\"value\":\"adsf\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"sort\":[{\"desc\":{\"order\":\"desc\"}}]}";
            String actRet5 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql5).toString();
            Assert.assertEquals(expRet5, actRet5);

            String expRet6 = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"dt_time\":{\"from\":\"202105091817+0800\",\"to\":null,\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"range\":{\"dt_time\":{\"from\":null,\"to\":\"202105111817+0800\",\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"wildcard\":{\"country\":{\"wildcard\":\"*中国黑龙江绥化*\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet6 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql6).toString();
            Assert.assertEquals(expRet6, actRet6);

            String expRet7 = "{\"query\":{\"bool\":{\"must\":[{\"range\":{\"dt_time\":{\"from\":\"202105092057+0800\",\"to\":null,\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"range\":{\"dt_time\":{\"from\":null,\"to\":\"202105112057+0800\",\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"term\":{\"act\":{\"value\":\"7609\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet7 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql7).toString();
            Assert.assertEquals(expRet7, actRet7);

            String expRet8 = "{\"from\":0,\"size\":10,\"query\":{\"bool\":{\"must\":[{\"range\":{\"dt_time\":{\"from\":\"202105092057+0800\",\"to\":null,\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"range\":{\"dt_time\":{\"from\":null,\"to\":\"202105112057+0800\",\"include_lower\":true,\"include_upper\":true,\"boost\":1.0}}},{\"term\":{\"act\":{\"value\":\"7609\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}}";
            String actRet8 = SQLToEsDSLUtils.sqlToEsDslQueryBody(sql8).toString();
            Assert.assertEquals(expRet8, actRet8);
            // 下面的包含 index信息，通过得到的这个 SearchRequest 这个实例 ，可以由该实例传到RestHighLevelClient#search 中去es查询获取结果
            System.out.println("转化sql1的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql1));
            System.out.println("转化sql2的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql2));
            System.out.println("转化sql3的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql3));
            System.out.println("转化sql4的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql4));
            System.out.println("转化sql5的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql5));
            System.out.println("转化sql6的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql6));
            System.out.println("转化sql7的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql7));
            System.out.println("转化sql8的请求参数结果：" + SQLToEsDSLUtils.sqlToEsSearchRequest(sql8));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    /**
     * 测试是否为计数sql
     */
    @Test
    public void testIsCountSql() {
        String sql1 = "select * from test_t ";
        String sql2 = "select count(1) as diff from test_t ";
        try {
            System.out.println("测试是否为计算的sql1:" + SQLToEsDSLUtils.isCountSql(sql1));
            System.out.println("测试是否为计算的sql2:" + SQLToEsDSLUtils.isCountSql(sql2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
