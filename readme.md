# elasticsearch-sql

这个项目主要是将ElasticSearch DSL 转成 SQL 或者将 SQL 转成 ElasticSearch DSL 的工具模块 

# 项目说明


### 特点

### 缺点


#  使用说明

主要使用这个工具类 ： SQLToEsDSLUtils


# 代码示例

```java
public class SqlToEsDslUtilsTest {
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
            System.out.println("转化sql1的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql1));
            System.out.println("转化sql2的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql2));
            System.out.println("转化sql3的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql3));
            System.out.println("转化sql4的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql4));
            System.out.println("转化sql5的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql5));
            System.out.println("转化sql6的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql6));
            System.out.println("转化sql7的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql7));
            System.out.println("转化sql8的结果：" + SQLToEsDSLUtils.sqlToEsDslQueryBody(sql8));
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
}
```

运行结果


# 项目功能搭建思路

