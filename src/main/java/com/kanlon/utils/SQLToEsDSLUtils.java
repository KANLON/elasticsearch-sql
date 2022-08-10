package com.kanlon.utils;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.util.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.druid.sql.SQLUtils.parseStatements;

/**
 * sql的语法转为 es dsl 的语法
 *
 * @author zhangcanlong
 * @since 2022/08/10 21:25
 **/
public class SQLToEsDSLUtils {

    private SQLToEsDSLUtils() {}

    /**
     * 表示所有列
     */
    public final static String ALL_COLUMN = "*";

    /**
     * 是否为计算的sql
     */
    private final static String COUNT_SELECT = "count(";

    /**
     * 判断是否为计数sql
     *
     * @param sql sql
     * @return boolean
     * @throws RuntimeException 仪表板异常
     */
    public static boolean isCountSql(String sql) throws RuntimeException {
        Map<String, String> columnMap = getEsColumnExpressMap(sql);
        if (!CollectionUtils.isEmpty(columnMap)) {
            String columnKey = columnMap.keySet().iterator().next().replaceAll("\\s", "");
            String columnValue = columnMap.values().iterator().next().replaceAll("\\s", "");
            return columnKey.contains(COUNT_SELECT) || columnValue.contains(COUNT_SELECT);
        }
        return false;
    }

    /**
     * sql 转成 es的查询对象
     *
     * @param sql sql
     * @return {@link SearchRequest}
     */
    public static SearchRequest sqlToEsSearchRequest(String sql) throws RuntimeException {
        SearchRequest rq = new SearchRequest();
        if (StringUtils.isEmpty(sql)) {
            return rq;
        }
        SearchSourceBuilder searchSourceBuilder = sqlToEsDslQueryBody(sql);
        List<String> tableNames = SelfDruidSqlUtils.getTableNamesBySql(sql, DbType.mysql);
        // 设置es的index为小写
        String[] indexArray = new String[tableNames.size()];
        for (int i = 0; i < indexArray.length; ++i) {
            indexArray[i] = tableNames.get(i).toLowerCase();
        }
        //索引
        rq.indices(indexArray);
        //各种组合条件
        rq.source(searchSourceBuilder);
        return rq;
    }


    /**
     * sql转成 es 查询的dsl中的查询参数 （直接调用该对象的 SearchSourceBuilder#toString() 方法即可得到参数的json字符串形式）
     *
     * @param sql sql
     * @return {@link SearchSourceBuilder}
     */
    public static SearchSourceBuilder sqlToEsDslQueryBody(String sql) throws RuntimeException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (StringUtils.isEmpty(sql)) {
            return sourceBuilder;
        }
        boolean isCountSql = isCountSql(sql);
        Map<String, String> columnMap = getEsColumnExpressMap(sql);
        if (CollectionUtils.isEmpty(columnMap)) {
            columnMap.put(ALL_COLUMN, ALL_COLUMN);
        }
        // 不是获取全部列 并且不是 计算sql 才需要过滤
        if (!Objects.equals(columnMap.entrySet().iterator().next().getKey(), ALL_COLUMN) && !isCountSql) {
            // 第一个参数表示结果集返回哪些字段，第二表示不返回哪些参数
            sourceBuilder.fetchSource(columnMap.keySet().toArray(new String[0]), new String[]{});
        }
        List<SQLStatement> stmtLists = parseStatements(sql, com.alibaba.druid.DbType.mysql);
        SQLSelectStatement tempStat = (SQLSelectStatement) stmtLists.iterator().next();
        // 如果有多个，取第一个queryBlock
        SQLSelectQuery sqlSelectQuery = tempStat.getSelect().getQuery();
        SQLSelectQueryBlock sqlSelectQueryBlock = new SQLSelectQueryBlock();

        // 获取分页条件
        SQLLimit sqlLimit = null;
        // 排序的条件
        SQLOrderBy sqlOrderBy = null;
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            SQLUnionQuery sqlUnionQuery = null;
            if (isCountSql && sqlTableSource instanceof SQLUnionQueryTableSource) {
                SQLUnionQueryTableSource sqlUnionQueryTableSource = (SQLUnionQueryTableSource) sqlTableSource;
                sqlUnionQuery = sqlUnionQueryTableSource.getUnion();
                SQLSelectQuery sqlSelectQueryRight = sqlUnionQuery.getRight();
                if (sqlSelectQueryRight instanceof SQLSelectQueryBlock) {
                    sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQueryRight;
                }
            }
            if (sqlUnionQuery == null) {
                sqlLimit = sqlSelectQueryBlock.getLimit();
                sqlOrderBy = sqlSelectQueryBlock.getOrderBy();
            } else {
                sqlLimit = sqlUnionQuery.getLimit();
                sqlOrderBy = sqlUnionQuery.getOrderBy();
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            // union all
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            sqlLimit = sqlUnionQuery.getLimit();
            sqlOrderBy = sqlUnionQuery.getOrderBy();
            // 如果是是计算sql，并且右边还是union all，则再取一层
            if (isCountSql && sqlUnionQuery.getRight() instanceof SQLUnionQuery) {
                SQLUnionQuery sqlUnionQueryRight = (SQLUnionQuery) sqlUnionQuery.getRight();
                if (sqlUnionQueryRight.getRight() instanceof SQLUnionQuery) {
                    throw new RuntimeException("目前仅支持计数sql的一层union all！请确保sql中只有一层union all");
                } else {
                    sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlUnionQueryRight.getRight();
                }
            } else {
                sqlSelectQueryBlock = getSelectQueryBlockFromSqlSelectQuery(sqlUnionQuery);
            }
        }
        SQLExpr whereSqlExpr = sqlSelectQueryBlock.getWhere();


        // 设置查询分页
        if (sqlLimit != null && !isCountSql) {
            sourceBuilder.from(sqlLimit.getOffset() == null ? 0 : Integer.parseInt(toEsQueryString(sqlLimit.getOffset())));
            sourceBuilder.size(sqlLimit.getRowCount() == null ? 0 : Integer.parseInt(toEsQueryString(sqlLimit.getRowCount())));
        }
        // 设置排序
        if (sqlOrderBy != null) {
            List<SQLSelectOrderByItem> sqlSelectOrderByItems = sqlOrderBy.getItems();
            for (SQLSelectOrderByItem orderByItem : sqlSelectOrderByItems) {
                String orderByColumn = toEsQueryString(orderByItem.getExpr());
                SQLOrderingSpecification orderingSpecification = orderByItem.getType();
                sourceBuilder.sort(orderByColumn, Objects.equals(orderingSpecification, SQLOrderingSpecification.ASC) ? SortOrder.ASC : SortOrder.DESC);
            }
        }
        //组建查询条件
        setEsBoolQueryBuilderBySqlWhereSqlExpr(whereSqlExpr, boolQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        return sourceBuilder;
    }

    /**
     * 根据sql  where 条件SQLExpr设置es BoolQueryBuilder 查询
     *
     * @param whereSqlExpr     在sql expr
     * @param boolQueryBuilder bool查询构建器
     */
    private static void setEsBoolQueryBuilderBySqlWhereSqlExpr(SQLExpr whereSqlExpr, BoolQueryBuilder boolQueryBuilder) {
        if (whereSqlExpr == null) {
            return;
        }
        // 只有是属于这三个对象的才能获取到条件
        boolean canGetCondition = whereSqlExpr instanceof SQLBinaryOpExpr || whereSqlExpr instanceof SQLInListExpr || whereSqlExpr instanceof SQLBetweenExpr;
        if (!canGetCondition) {
            return;
        }
        SQLBinaryOpExpr sqlBinaryOpExpr;
        if (whereSqlExpr instanceof SQLBinaryOpExpr) {
            sqlBinaryOpExpr = (SQLBinaryOpExpr) whereSqlExpr;
            SQLExpr sqlExprLeft = sqlBinaryOpExpr.getLeft();
            SQLExpr sqlExprRight = sqlBinaryOpExpr.getRight();
            boolean leftCanGetCondition = sqlExprLeft instanceof SQLBinaryOpExpr || sqlExprLeft instanceof SQLInListExpr || sqlExprLeft instanceof SQLBetweenExpr;
            boolean rightCanGetCondition = sqlExprRight instanceof SQLBinaryOpExpr || sqlExprRight instanceof SQLInListExpr || sqlExprRight instanceof SQLBetweenExpr;
            // 只有左右都不是能分解的才能加上条件
            if (!leftCanGetCondition && !rightCanGetCondition) {
                if (SQLBinaryOperator.Equality == sqlBinaryOpExpr.getOperator()) {
                    boolQueryBuilder.must(QueryBuilders.termQuery(toEsQueryString(sqlExprLeft), toEsQueryString(sqlExprRight)));
                } else if (SQLBinaryOperator.GreaterThan == sqlBinaryOpExpr.getOperator()) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(toEsQueryString(sqlExprLeft)).gt(toEsQueryString(sqlExprRight)));
                } else if (SQLBinaryOperator.GreaterThanOrEqual == sqlBinaryOpExpr.getOperator()) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(toEsQueryString(sqlExprLeft)).gte(toEsQueryString(sqlExprRight)));
                } else if (SQLBinaryOperator.LessThanOrEqual == sqlBinaryOpExpr.getOperator()) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(toEsQueryString(sqlExprLeft)).lte(toEsQueryString(sqlExprRight)));
                } else if (SQLBinaryOperator.LessThan == sqlBinaryOpExpr.getOperator()) {
                    boolQueryBuilder.must(QueryBuilders.rangeQuery(toEsQueryString(sqlExprLeft)).lt(toEsQueryString(sqlExprRight)));
                } else if (SQLBinaryOperator.Like == sqlBinaryOpExpr.getOperator()) {
                    String sqlRight = toEsQueryString(sqlExprRight);
                    String allLike = "%%";
                    if (!StringUtils.isEmpty(sqlRight) && !Objects.equals(sqlRight, allLike)) {
                        sqlRight = '*' + sqlRight.substring(1, sqlRight.length() - 1) + '*';
                        boolQueryBuilder.must(QueryBuilders.wildcardQuery(toEsQueryString(sqlExprLeft), sqlRight));
                    }
                }
            }
            if (leftCanGetCondition) {
                setEsBoolQueryBuilderBySqlWhereSqlExpr(sqlExprLeft, boolQueryBuilder);

            }
            if (rightCanGetCondition) {
                setEsBoolQueryBuilderBySqlWhereSqlExpr(sqlExprRight, boolQueryBuilder);
            }
        } else if (whereSqlExpr instanceof SQLInListExpr) {
            SQLInListExpr sqlInListExpr = (SQLInListExpr) whereSqlExpr;
            List<SQLExpr> sqlExprs = sqlInListExpr.getTargetList();
            List<String> inList = new ArrayList<>();
            for (SQLExpr sqlExpr : sqlExprs) {
                inList.add(toEsQueryString(sqlExpr));
            }
            // 如果是不在的 话，则not in
            if (sqlInListExpr.isNot()) {
                boolQueryBuilder.mustNot(QueryBuilders.termsQuery(toEsQueryString(sqlInListExpr.getExpr()), inList));
            } else {
                boolQueryBuilder.must(QueryBuilders.termsQuery(toEsQueryString(sqlInListExpr.getExpr()), inList));
            }
        } else {
            // 这里一定为 SQLBetweenExpr 的，不然不会到这里
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) whereSqlExpr;
            boolQueryBuilder.must(QueryBuilders.rangeQuery(toEsQueryString(sqlBetweenExpr.getTestExpr())).from(toEsQueryString(sqlBetweenExpr.getBeginExpr())).to(toEsQueryString(sqlBetweenExpr.getEndExpr())));
        }
    }

    /**
     * 得到es的sql列的表达式， 去掉别名的 ``
     *
     * @param sql sql
     * @return {@link Map}  key 为列别名，value 为实际的列
     * @throws RuntimeException 仪表板异常
     */
    public static Map<String, String> getEsColumnExpressMap(String sql) throws RuntimeException {
        Map<String, String> originColumnMap = SelfDruidSqlUtils.getColumnExpressMap(sql, DbType.mysql);
        Map<String, String> columnMap = new HashMap<>(16);
        for (Map.Entry<String, String> entry : originColumnMap.entrySet()) {
            columnMap.put(entry.getKey().replace("`", ""), entry.getValue());
        }
        return columnMap;
    }

    /**
     * 将原本的sql 字符串 转为es查询字符串
     *
     * @param sqlExpr sql expr
     * @return {@link String}
     */
    private static String toEsQueryString(SQLExpr sqlExpr) {
        if (sqlExpr == null) {
            return "";
        }
        if (sqlExpr instanceof SQLCharExpr) {
            SQLCharExpr sqlCharExpr = (SQLCharExpr) sqlExpr;
            return sqlCharExpr.getText();
        }
        return SQLUtils.toSQLString(sqlExpr);
    }

    /**
     * 从sql select查询 获取到随意个 SQLSelectQueryBlock sql 语句
     *
     * @param sqlSelectQuery sql select查询
     * @return {@link SQLSelectQueryBlock}
     */
    private static SQLSelectQueryBlock getSelectQueryBlockFromSqlSelectQuery(SQLSelectQuery sqlSelectQuery) throws RuntimeException {
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            return (SQLSelectQueryBlock) sqlSelectQuery;
        }
        if (sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            SQLSelectQuery sqlSelectQueryLeft = sqlUnionQuery.getLeft();
            SQLSelectQuery sqlSelectQueryRight = sqlUnionQuery.getRight();
            if (sqlSelectQueryLeft != null) {
                return getSelectQueryBlockFromSqlSelectQuery(sqlSelectQueryLeft);
            }
            if (sqlSelectQueryRight != null) {
                return getSelectQueryBlockFromSqlSelectQuery(sqlSelectQueryRight);
            }
        }
        throw new RuntimeException("无法解析sql！解析出来的sql类型为：" + sqlSelectQuery.getClass());
    }


}
