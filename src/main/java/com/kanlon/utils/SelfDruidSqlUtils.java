package com.kanlon.utils;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.druid.sql.SQLUtils.parseStatements;
import static com.alibaba.druid.sql.SQLUtils.toSQLExpr;
import static com.alibaba.druid.sql.SQLUtils.toSQLString;

/**
 * 自定义的druid相关的解析的sql工具类，基于SQLUtils
 *
 * @author zhangcanlong
 * @since 2022/08/10 21:25
 **/
@Slf4j
public class SelfDruidSqlUtils {

    /**
     * 限制只能通过类名调用
     */
    private SelfDruidSqlUtils() {}

    /**
     * 获取到表行数的 列名
     **/
    public static final String TABLE_ROW_COLUMN = "table_rows";

    /**
     * 计算获取某个sql 的总条数的sql，中的新列
     */
    public static final String NUM_STR = "num";

    /**
     * global in 替换的列 字段
     */
    public static final String GLOBAL_IN_REPLACE_COLUMN_STR = "_dashboard_global_in_column";

    /**
     * global not in 替换的列 字段
     */
    public static final String GLOBAL_NOT_IN_REPLACE_COLUMN_STR = "_dashboard_global_not_in_column";


    /**
     * sql解析中包含剩余列的表名，如果再解析出的map中找不到字段名，才拼装成上这个表名，作为字段名
     */
    public static final String SQL_PARSE_ALL_TABLE_NAME = "ds_AllTableColumns__tableName";

    /**
     * 双引号
     */
    public static char DOUBLE_QUOTE = '"';
    /**
     * 空 值
     */
    public static final String EMPTY = "";

    /**
     * 星号
     */
    public static final String ASTERISK = "*";
    /**
     * 点
     */
    public static final String DOT = ".";
    /**
     * 撇号
     */
    public static final String BACKTICK = "`";

    /**
     * 在sql重新替换 会  包含 global in  特殊 列的sql，添加上 global in
     *
     * @param sql sql 要重新替换的sql（与SelfDruidSqlUtils#replaceGlobalInSql 对应）
     * @return {@link String}
     */
    public static String replaceGlobalInColumnSql(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return sql;
        }
        sql = sql.replaceAll(GLOBAL_NOT_IN_REPLACE_COLUMN_STR, " global not ");
        sql = sql.replaceAll(GLOBAL_IN_REPLACE_COLUMN_STR, " global ");
        return sql;
    }

    /**
     * 在sql取代 global in的sql
     *
     * @param sql sql 要替换的sql （与SelfDruidSqlUtils#replaceGlobalInColumnSql 对应）
     * @return {@link String}
     */
    public static String replaceGlobalInSql(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return sql;
        }
        sql = sql.replaceAll("\\s+(global|GLOBAL)\\s{1,4}(not|NOT)\\s{1,4}(IN|in)", GLOBAL_NOT_IN_REPLACE_COLUMN_STR + " in");
        sql = sql.replaceAll("\\s+(global|GLOBAL)\\s{1,4}(IN|in)", GLOBAL_IN_REPLACE_COLUMN_STR + " in");
        return sql;
    }

    /**
     * 根据表名获取表的的大概行数的sql
     *
     * @param tableName 表名
     * @return 获取表行数的sql
     **/
    public static String getMysqlTableRowNumSql(String tableName) {
        if (StringUtils.isBlank(tableName)) {
            return EMPTY;
        }
        tableName = tableName.replace(BACKTICK, EMPTY).toLowerCase();
        String dbName;
        String actualTableName;
        // 如果包含数据库
        if (tableName.contains(DOT)) {
            int dotIndex = tableName.indexOf(DOT);
            dbName = tableName.substring(0, dotIndex);
            actualTableName = tableName.substring(dotIndex + 1);
            return "SELECT " + TABLE_ROW_COLUMN + " FROM information_schema.tables  WHERE LOWER(TABLE_SCHEMA) = '" + dbName + "'  AND LOWER(table_name)='" + actualTableName + "'";
        }
        return "SELECT " + TABLE_ROW_COLUMN + " FROM INFORMATION_SCHEMA.PARTITIONS WHERE LOWER(TABLE_NAME)='" + tableName + "'";
    }

    /**
     * 得到实际的列 名称
     *
     * @param columnExpressMap 解析后的sql列对应关系
     * @param column           列名
     * @return 实际的列名称
     */
    public static String getActualColumn(Map<String, String> columnExpressMap, String column) {
        // 如果包含了全部列的表，则拼装上表名，否则获取原始字段名
        if (columnExpressMap.containsKey(SQL_PARSE_ALL_TABLE_NAME)) {
            return columnExpressMap.getOrDefault(column, columnExpressMap.get(SQL_PARSE_ALL_TABLE_NAME) + DOT + column);
        } else {
            return columnExpressMap.getOrDefault(column, column);
        }
    }

    /**
     * 处理程序sql expr中的列名
     *
     * @param sqlExpr         sql expr
     * @param actualColumnMap 实际列映射
     * @param dbType          db型
     */
    public static void handlerSqlExprColumnName(SQLExpr sqlExpr, Map<String, String> actualColumnMap, DbType dbType) {
        if (sqlExpr instanceof SQLInListExpr) {
            SQLInListExpr sqlInListExpr = (SQLInListExpr) sqlExpr;
            SQLExpr inSqlExpr1 = sqlInListExpr.getExpr();
            if (inSqlExpr1 instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) inSqlExpr1;
                String name = sqlIdentifierExpr.getName();
                String actualName = SelfDruidSqlUtils.getActualColumn(actualColumnMap, name);
                sqlInListExpr.setExpr(SQLUtils.toSQLExpr(actualName, dbType));
            }
        } else if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLExpr leftSqlExpr = sqlBinaryOpExpr.getLeft();
            SQLExpr rightSqlExpr = sqlBinaryOpExpr.getRight();

            if (leftSqlExpr instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) leftSqlExpr;
                String name = sqlIdentifierExpr.getName();
                String actualName = SelfDruidSqlUtils.getActualColumn(actualColumnMap, name);
                sqlBinaryOpExpr.setLeft(SQLUtils.toSQLExpr(actualName, dbType));
            } else {
                handlerSqlExprColumnName(leftSqlExpr, actualColumnMap, dbType);
            }
            handlerSqlExprColumnName(rightSqlExpr, actualColumnMap, dbType);
        }
    }

    /**
     * 解析sql获取sql中列的字段别名及其对应的表达式之前的关系，支持 单个select sql 和 union all sql
     * (注意如果查询中包含多个全部列查询*，例如：t.*,t2.* 则有可能会出错的，会把未归属的列，归属到t2中）
     *
     * @param sql 要解析sql
     * @return 字段别名及其对应的表达式的map，如果包含别名才放入该集合
     **/
    public static Map<String, String> getColumnExpressMap(String sql, com.alibaba.druid.DbType dbType) throws RuntimeException {
        Map<String, String> columnExpressMap = new HashMap<>(16);
        List<SQLStatement> sqlStatements;
        try {
            SQLSelectQueryBlock sqlSelectQueryBlock;
            sqlStatements = parseStatements(sql, dbType);
            SQLSelectQuery sqlSelectQuery = (((SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1)).getSelect()).getQuery();
            if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
                sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            } else if (sqlSelectQuery instanceof SQLUnionQuery) {
                // union all
                SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
                return getColumnExpressMap(SQLUtils.toSQLString(sqlUnionQuery.getLeft(), dbType), dbType);
            } else {
                throw new RuntimeException("无法解析sql！请更换sql或者联系系统管理员咨询支持的sql类型！");
            }
            List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
            // 查询全部列的查询项的个数，例如：t.*，则算一个
            int allColumnSelectCnt = 0;
            // 遍历条件项及获取表达式及其对应的字段别名
            for (SQLSelectItem selectItem : selectItems) {
                String columnAlias = selectItem.getAlias();
                if (StringUtils.isNotEmpty(columnAlias)) {
                    columnAlias = columnAlias.replaceAll("['`]", EMPTY);
                    // 如果开头和结尾是 " 也去掉
                    if (Objects.equals(columnAlias.charAt(0), DOUBLE_QUOTE) && Objects.equals(columnAlias.charAt(columnAlias.length() - 1), DOUBLE_QUOTE)) {
                        columnAlias = columnAlias.replace(String.valueOf(DOUBLE_QUOTE), EMPTY);
                    }
                    columnExpressMap.put(columnAlias, SQLUtils.toSQLString(selectItem.getExpr()));
                } else {
                    if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) selectItem.getExpr();
                        if (Objects.equals(sqlPropertyExpr.getName(), ASTERISK)) {
                            allColumnSelectCnt++;
                            columnExpressMap.put(SQL_PARSE_ALL_TABLE_NAME, sqlPropertyExpr.getOwnerName());
                        } else {
                            columnExpressMap.put(((SQLPropertyExpr) selectItem.getExpr()).getName().replaceAll("['`]", EMPTY), SQLUtils.toSQLString(selectItem.getExpr()));
                        }
                    } else {
                        columnExpressMap.put(SQLUtils.toSQLString(selectItem.getExpr()), SQLUtils.toSQLString(selectItem.getExpr()));
                    }
                }
            }
            // 如果查询全部列的查询项大于1，则去掉，查询全部列表的 key
            if (allColumnSelectCnt > 1) {
                columnExpressMap.remove(SQL_PARSE_ALL_TABLE_NAME);
            }
        } catch (Exception e) {
            log.error("解析SQL错误！要解析的sql为【{}】", sql);
            throw new RuntimeException("解析SQL错误！请确认SQL中字段关键字使用``和''合理括起来了！" + e.getMessage(), e);
        }
        return columnExpressMap;
    }

    /**
     * 获取某个sql的中表名
     *
     * @param sql    要解析的sql的表名
     * @param dbType 数据库类型
     * @return 表名集合
     **/
    public static List<String> getTableNamesBySql(String sql, DbType dbType) throws RuntimeException {
        List<String> tableNameList = new ArrayList<>(10);
        if (StringUtils.isBlank(sql)) {
            return tableNameList;
        }
        try {
            // 将 global in 替换
            sql = replaceGlobalInSql(sql);
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            for (SQLStatement stmt : stmtList) {
                SchemaStatVisitor schemaStatVisitor = new SchemaStatVisitor();
                stmt.accept(schemaStatVisitor);
                //获取表名称
                Map<TableStat.Name, TableStat> nameTableStatMap = schemaStatVisitor.getTables();
                for (TableStat.Name name : nameTableStatMap.keySet()) {
                    tableNameList.add(name.toString());
                }
            }
        } catch (Exception e) {
            log.error("解析SQL错误！要解析的sql为【{}】", sql);
            throw new RuntimeException("解析SQL错误！" + e.getMessage(), e);
        }
        return tableNameList;
    }

    /**
     * 替换sql中的查询项(仅支持，单个select的sql，如果为union all，则直接 在外面再嵌套一层 select *)
     *
     * @param sql          要替换查询项的sql
     * @param express      替换后的查询表达式
     * @param alias        替换后的查询别名(如果为null，则只设置的查询表达式)
     * @param haveDistinct 表达式是否有distinct,如果包含distinct表达式，需要另外处理
     * @param dbType       数据库类型
     * @return 替换后sql查询项的sql
     **/
    public static String replaceSelectItem(String sql, String express, String alias, boolean haveDistinct, DbType dbType) throws RuntimeException {
        SQLSelectQueryBlock sqlSelectQueryBlock;
        SQLSelectStatement sqlSelectStatement;
        SQLSelect sqlSelect;
        SQLWithSubqueryClause withSubqueryClause;
        try {
            List<SQLStatement> sqlStatements = parseStatements(sql, dbType);
            sqlSelectStatement = (SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1);
            Map<String, String> columnMap = getColumnExpressMap(sql, dbType);
            sqlSelect = sqlSelectStatement.getSelect();
            SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
            withSubqueryClause = sqlSelect.getWithSubQuery();
            if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
                sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            } else if (sqlSelectQuery instanceof SQLUnionQuery) {
                sql = "select * from (" + sql + ") temp_t";
                List<SQLStatement> unionAllSqlStatements = parseStatements(sql, dbType);
                SQLSelectQuery tempSqlSelectQuery = (((SQLSelectStatement) unionAllSqlStatements.get(unionAllSqlStatements.size() - 1)).getSelect()).getQuery();
                sqlSelectQueryBlock = (SQLSelectQueryBlock) tempSqlSelectQuery;
            } else {
                throw new RuntimeException("无法解析sql！请更换sql或者联系系统管理员咨询支持的sql类型！");
            }
            SQLSelectGroupByClause selectGroupByClause = sqlSelectQueryBlock.getGroupBy();
            if (selectGroupByClause != null) {
                List<SQLExpr> groupBySqlExprList = selectGroupByClause.getItems();
                for (int i = 0; i < groupBySqlExprList.size(); ++i) {
                    groupBySqlExprList.set(i, SQLUtils.toSQLExpr(SelfDruidSqlUtils.getActualColumn(columnMap, SQLUtils.toSQLString(groupBySqlExprList.get(i))), dbType));
                }
            }
            String tempSql;
            if (!StringUtils.isEmpty(alias)) {
                tempSql = "select " + express + " as " + alias + " from temp1";
            } else {
                tempSql = "select " + express + " from temp1";
            }
            List<SQLStatement> tempSqlStatements = parseStatements(tempSql, dbType);
            SQLSelectQuery tempSelectQuery = (((SQLSelectStatement) tempSqlStatements.get(tempSqlStatements.size() - 1)).getSelect()).getQuery();
            sqlSelectQueryBlock.getSelectList().clear();
            SQLSelectQueryBlock tempDistinctColumnQuery = (SQLSelectQueryBlock) tempSelectQuery;
            for (SQLSelectItem selectItem : tempDistinctColumnQuery.getSelectList()) {
                sqlSelectQueryBlock.getSelectList().add(selectItem);
            }
            if (haveDistinct) {
                sqlSelectQueryBlock.setDistinct();
            }
        } catch (Exception e) {
            log.error("替换查询列，解析SQL错误！要解析的sql为【{}】", sql);
            throw new RuntimeException("解析SQL错误！" + e.getMessage(), e);
        }
        return (withSubqueryClause == null ? "" : SQLUtils.toSQLString(withSubqueryClause)) + "\n" + SQLUtils.toSQLString(sqlSelectQueryBlock, dbType);
    }

    /**
     * 添加sql 的查询项
     *
     * @param sql           sql
     * @param selectItemSql 选择项sql
     * @param dbType        db型
     * @return {@link String}
     */
    public static String addSqlSelectItem(String sql, String selectItemSql, DbType dbType) throws RuntimeException {
        String haveAddSqlSelectItemSql = sql;
        if (StringUtils.isEmpty(sql) || StringUtils.isEmpty(selectItemSql)) {
            return haveAddSqlSelectItemSql;
        }
        String[] needAddSelectItems = selectItemSql.split(" as|AS ");
        String needAddSelectSql = needAddSelectItems[0];
        String needAddSelectAlias = needAddSelectItems.length > 1 ? needAddSelectItems[1] : null;
        String actualSelectColumn = StringUtils.isEmpty(needAddSelectAlias) ? needAddSelectSql : needAddSelectAlias;
        Map<String, String> columnMap;
        try {
            columnMap = getColumnExpressMap(sql, dbType);
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        // 如果sql中已经存在了该列，则直接返回
        if (columnMap.containsKey(actualSelectColumn)) {
            return sql;
        }
        haveAddSqlSelectItemSql = SQLUtils.addSelectItem(sql, needAddSelectSql, needAddSelectAlias, dbType);
        return haveAddSqlSelectItemSql;
    }


    /**
     * 删除sql的某些查询项
     *
     * @param sql                       sql
     * @param needRemoveSelectColumnSet 需要删除选择列集
     * @param dbType                    数据库类型
     * @return 删除某些查询项返回的sql
     */
    public static String removeSqlSelectItem(String sql, Set<String> needRemoveSelectColumnSet, DbType dbType) throws RuntimeException {
        if (CollectionUtils.isEmpty(needRemoveSelectColumnSet)) {
            return sql;
        }
        SQLSelectQueryBlock sqlSelectQueryBlock;
        SQLSelectStatement sqlSelectStatement;
        SQLSelect sqlSelect;
        SQLWithSubqueryClause withSubQueryClause;
        List<SQLStatement> sqlStatements = parseStatements(sql, dbType);
        sqlSelectStatement = (SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1);
        sqlSelect = sqlSelectStatement.getSelect();
        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
        withSubQueryClause = sqlSelect.getWithSubQuery();
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
        } else {
            throw new RuntimeException("不支持去除该SQL类型的查询项！该SQL类型为：" + sqlSelectQuery.getClass());
        }
        List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
        Iterator<SQLSelectItem> itemIterator = selectItems.iterator();
        while (itemIterator.hasNext()) {
            SQLSelectItem sqlSelectItem = itemIterator.next();
            SQLExpr sqlExpr = sqlSelectItem.getExpr();
            String alias = SelfDruidSqlUtils.getRealAlias(sqlSelectItem.getAlias());
            String needFilterSelectValue = StringUtils.isBlank(alias) ? SQLUtils.toSQLString(sqlExpr) : alias;
            if (needRemoveSelectColumnSet.contains(needFilterSelectValue)) {
                itemIterator.remove();
            }
        }
        return (withSubQueryClause == null ? "" : SQLUtils.toSQLString(withSubQueryClause)) + "\n" + SQLUtils.toSQLString(sqlSelectQuery, dbType);
    }


    /**
     * 只保留 ，指定某个查询列的sql(慎用，会有很多坑，尽量通过过滤的列实现)
     *
     * @param sql              sql
     * @param needSelectColumn 需要选择列
     * @param dbType           db型
     * @return {@link String}
     * @throws RuntimeException 一般例外
     */
    public static String onlySaveAppointSqlSelectItem(String sql, Set<String> needSelectColumn, DbType dbType) throws RuntimeException {
        SQLSelectQueryBlock sqlSelectQueryBlock;
        SQLSelectStatement sqlSelectStatement;
        SQLSelect sqlSelect;
        SQLWithSubqueryClause withSubQueryClause;
        List<SQLStatement> sqlStatements = parseStatements(sql, dbType);
        sqlSelectStatement = (SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1);
        sqlSelect = sqlSelectStatement.getSelect();
        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();
        withSubQueryClause = sqlSelect.getWithSubQuery();
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
        } else {
            throw new RuntimeException("不支持只保留该SQL类型的查询项！该SQL类型为：" + sqlSelectQuery.getClass());
        }
        List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
        Iterator<SQLSelectItem> iterator = selectItems.iterator();
        while (iterator.hasNext()) {
            SQLSelectItem sqlSelectItem = iterator.next();
            SQLExpr sqlExpr = sqlSelectItem.getExpr();
            String alias = SelfDruidSqlUtils.getRealAlias(sqlSelectItem.getAlias());
            String needFilterSelectValue = StringUtils.isBlank(alias) ? SQLUtils.toSQLString(sqlExpr) : alias;
            // 如果不在指定的列中，则删除；
            if (!needSelectColumn.contains(needFilterSelectValue)) {
                iterator.remove();
            } else {
                // 一旦有了 查询了某列，则从指定集合中去掉，方便以免后面添加的时候，又添加一次
                needSelectColumn.remove(needFilterSelectValue);
            }
        }
        // 最后添加上所有 没有查询到的列名
        Map<String, String> columnExpressMap;
        try {
            columnExpressMap = SelfDruidSqlUtils.getColumnExpressMap(sql, dbType);
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        needSelectColumn.removeAll(columnExpressMap.keySet());
        for (String sqlSelectColumn : needSelectColumn) {
            SQLSelectItem sqlSelectItem = new SQLSelectItem(SQLUtils.toSQLExpr(sqlSelectColumn, dbType));
            selectItems.add(sqlSelectItem);
        }

        return (withSubQueryClause == null ? "" : SQLUtils.toSQLString(withSubQueryClause)) + "\n" + SQLUtils.toSQLString(sqlSelectQuery, dbType);
    }


    /**
     * 是否是 union  的sql，一般是union 的sql， 在排序的时候，字段只能为查询出来的字段
     *
     * @param sqlContent sql内容
     * @param dbType     druid的数据库类型
     * @return boolean
     */
    public static boolean isUnionSql(String sqlContent, DbType dbType) {
        List<SQLStatement> sqlStatements = parseStatements(sqlContent, dbType);
        SQLSelectQuery sqlSelectQuery = (((SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1)).getSelect()).getQuery();
        return sqlSelectQuery instanceof SQLUnionQuery;
    }

    /**
     * 检查sql中是否包含group by 条件(仅支持，单个select的sql，不支持union all)
     *
     * @param sql    要检查的sql
     * @param dbType 数据库类型
     * @return true 包含，false 不包含
     **/
    public static boolean isContainGroupBy(String sql, DbType dbType) throws RuntimeException {
        SQLSelectQueryBlock sqlSelectQueryBlock;
        try {
            List<SQLStatement> sqlStatements = parseStatements(sql, dbType);
            SQLSelectQuery sqlSelectQuery = (((SQLSelectStatement) sqlStatements.get(sqlStatements.size() - 1)).getSelect()).getQuery();
            if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
                sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            } else if (sqlSelectQuery instanceof SQLUnionQuery) {
                return false;
            } else {
                throw new RuntimeException("无法解析sql！请更换sql或者联系系统管理员咨询支持的sql类型！");
            }
            SQLSelectGroupByClause selectGroupByClause = sqlSelectQueryBlock.getGroupBy();
            return selectGroupByClause != null;
        } catch (Exception e) {
            log.error("检查sql中是否包含group by 条件，解析SQL错误！要解析的sql为【{}】", sql);
            throw new RuntimeException("解析SQL错误！" + e.getMessage(), e);
        }
    }

    /**
     * 动态增加SQL的 where 条件(sql 只支持单条的查询sql,支持union all的sql,如果是union all的sql，则每条sql都会添加条件，如果要全局的sql添加条件，在请直接使用使用：SQLSelectStatement.addWhere的方法)
     *
     * @param sql       SQL 语句
     * @param condition where 条件
     * @param dbType    DB类型
     */
    public static String addWhereForAllSql(String sql, String condition, DbType dbType) {
        TemplateBuilderSqlExpr templateBuilderSqlExpr = handleConditionAndSql(sql, condition, dbType);
        if (templateBuilderSqlExpr == null) {
            return sql;
        }
        StringBuilder builder = templateBuilderSqlExpr.getBuilder();
        for (int i = 0; i < templateBuilderSqlExpr.getStatements().size(); ++i) {
            addWhere(templateBuilderSqlExpr.getStatements().get(i), templateBuilderSqlExpr.getConditionExpr());
            builder.append(toSQLString(templateBuilderSqlExpr.getStatements().get(i), dbType));
            // 如果是多条sql，并且不是最后一条sql，则添加;
            if (i != templateBuilderSqlExpr.getStatements().size() - 1 && templateBuilderSqlExpr.isMultiStatement()) {
                builder.append(";");
            }
        }
        return builder.toString();
    }


    /**
     * 动态增加SQL的 where 条件(sql 只支持单条的查询sql,支持union all的sql,如果是union all的sql，则只给全局的sql添加条件
     *
     * @param sql       SQL 语句
     * @param condition where 条件
     * @param dbType    DB类型
     */
    public static String addHavingForGlobalSql(String sql, String condition, DbType dbType) {
        TemplateBuilderSqlExpr templateBuilderSqlExpr = handleConditionAndSql(sql, condition, dbType);
        if (templateBuilderSqlExpr == null) {
            return sql;
        }
        List<SQLStatement> statements = templateBuilderSqlExpr.getStatements();
        for (int i = 0; i < statements.size(); ++i) {
            if (statements.get(i) instanceof SQLSelectStatement) {
                SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statements.get(i);
                SQLSelectQuery query = sqlSelectStatement.getSelect().getQuery();
                // 如果是union all 的sql，则添加两次条件，因为第一次为添加1=1，用select * 包围原来的union all
                if (query instanceof SQLUnionQuery || query instanceof SQLSelectQueryBlock) {
                    addHavingForQuery(condition, dbType, query);
                } else {
                    throw new IllegalArgumentException("要添加having条件的sql为非查询 union all 或普通select sql，不能添加！sql内容为：" + sql);
                }
            } else {
                throw new IllegalArgumentException("要添加having条件的sql为非查询sql，不能添加！");
            }
            templateBuilderSqlExpr.getBuilder().append(toSQLString(statements.get(i), dbType));
            // 如果是多条sql，并且不是最后一条sql，则添加;
            if (i != statements.size() - 1 && templateBuilderSqlExpr.isMultiStatement()) {
                templateBuilderSqlExpr.getBuilder().append(";");
            }
        }
        return templateBuilderSqlExpr.getBuilder().toString();
    }


    /**
     * 动态增加SQL的 where 条件(sql 只支持单条的查询sql,支持union all的sql,如果是union all的sql，则只给全局的sql添加条件
     *
     * @param sql       SQL 语句
     * @param condition where 条件
     * @param dbType    DB类型
     */
    public static String addWhereForGlobalSql(String sql, String condition, DbType dbType) {
        TemplateBuilderSqlExpr templateBuilderSqlExpr = handleConditionAndSql(sql, condition, dbType);
        if (templateBuilderSqlExpr == null) {
            return sql;
        }
        List<SQLStatement> statements = templateBuilderSqlExpr.getStatements();
        for (int i = 0; i < statements.size(); ++i) {
            if (statements.get(i) instanceof SQLSelectStatement) {
                SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statements.get(i);
                SQLSelectQuery query = sqlSelectStatement.getSelect().getQuery();
                // 如果是union all 的sql，则添加两次条件，因为第一次为添加1=1，用select * 包围原来的union all
                if (query instanceof SQLUnionQuery) {
                    sqlSelectStatement.addWhere(toSQLExpr("1=1"));
                    sqlSelectStatement.addWhere(templateBuilderSqlExpr.getConditionExpr());
                } else if (query instanceof SQLSelectQueryBlock) {
                    sqlSelectStatement.addWhere(templateBuilderSqlExpr.getConditionExpr());
                } else {
                    throw new IllegalArgumentException("要添加条件的sql为非查询 union all 或普通select sql，不能添加！sql内容为：" + sql);
                }
            } else {
                throw new IllegalArgumentException("要添加条件的sql为非查询sql，不能添加！");
            }
            templateBuilderSqlExpr.getBuilder().append(toSQLString(statements.get(i), dbType));
            // 如果是多条sql，并且不是最后一条sql，则添加;
            if (i != statements.size() - 1 && templateBuilderSqlExpr.isMultiStatement()) {
                templateBuilderSqlExpr.getBuilder().append(";");
            }
        }
        return templateBuilderSqlExpr.getBuilder().toString();
    }

    /**
     * 增加where 条件（如果是union all的sql，则每条sql都会添加条件，如果要全局的sql添加条件，在请直接使用使用：SQLSelectStatement.addWhere的方法）
     *
     * @param stmt           查询的stmt
     * @param whereCondition where 的条件
     **/
    public static void addWhere(SQLStatement stmt, SQLExpr whereCondition) {
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            addWhereForSqlSelectQuery(query, whereCondition);
        } else {
            throw new IllegalArgumentException("add where not support " + stmt.getClass().getName());
        }
    }

    /**
     * 动态增加SQL的 group by 条件(sql 只支持单条的查询sql,暂不支持union all的sql)
     *
     * @param sql              SQL 语句
     * @param groupByCondition groupBy 条件
     * @param dbType           DB类型
     */
    public static String addGroupBy(String sql, String groupByCondition, DbType dbType) {
        TemplateBuilderSqlExpr templateBuilderSqlExpr = handleConditionAndSql(sql, groupByCondition, dbType);
        if (templateBuilderSqlExpr == null) {
            return sql;
        }
        List<SQLStatement> statements = templateBuilderSqlExpr.getStatements();
        SQLExpr conditionExpr = templateBuilderSqlExpr.getConditionExpr();
        boolean isMultiStatement = templateBuilderSqlExpr.isMultiStatement();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < statements.size(); ++i) {
            addGroupBy(statements.get(i), conditionExpr);
            builder.append(toSQLString(statements.get(i), dbType));
            // 如果是多条sql，并且不是最后一条sql，则添加;
            if (i != statements.size() - 1 && isMultiStatement) {
                builder.append(";");
            }
        }
        return builder.toString();
    }

    /**
     * 获取sql中排序字段的集合， 按照order by字段的顺序返回
     *
     * @param sql    sql
     * @param dbType db类型
     * @return {@link Set}<{@link String}> order by的字段集合
     */
    public static Set<String> listOrderByFieldSet(String sql, DbType dbType) {
        Set<String> orderByFieldSet = new LinkedHashSet<>(16);
        if (StringUtils.isEmpty(sql)) {
            return orderByFieldSet;
        }
        List<SQLStatement> statements = parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(statements)) {
            throw new IllegalArgumentException("要添加sql不包含sql语句!");
        }
        SQLStatement sqlStatement = statements.iterator().next();
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
            SQLSelectQuery query = sqlSelectStatement.getSelect().getQuery();
            SQLOrderBy sqlOrderBy;
            if (query instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
                sqlOrderBy = queryBlock.getOrderBy();
            } else if (query instanceof SQLUnionQuery) {
                SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) query;
                sqlOrderBy = sqlUnionQuery.getOrderBy();
            } else {
                throw new IllegalArgumentException("add where not support " + query.getClass().getName());
            }
            if (sqlOrderBy != null) {
                List<SQLSelectOrderByItem> sqlSelectOrderByItems = Optional.ofNullable(sqlOrderBy.getItems()).orElse(new ArrayList<>(0));
                orderByFieldSet = sqlSelectOrderByItems.stream().map(a -> SQLUtils.toSQLString(a.getExpr())).collect(Collectors.toCollection(LinkedHashSet::new));
            }
        } else {
            throw new RuntimeException("不支持该sql类型！");
        }
        return orderByFieldSet;
    }

    /**
     * 动态增加 SQL
     *
     * @param sql              select 查询的SQL 语句
     * @param orderByCondition 排序条件
     * @param dbType           DB类型
     */
    public static String addOrderBy(String sql, String orderByCondition, DbType dbType) {
        if (StringUtils.isEmpty(sql) || StringUtils.isEmpty(orderByCondition)) {
            return sql;
        }
        List<SQLStatement> statements = parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(statements)) {
            throw new IllegalArgumentException("要添加sql不包含sql语句!");
        }
        SQLSelectOrderByItem orderByItem = SQLUtils.toOrderByItem(orderByCondition, dbType);
        StringBuilder builder = new StringBuilder();
        boolean isMultiStatement = statements.size() > 1;
        for (int i = 0; i < statements.size(); ++i) {
            addOrderBy(statements.get(i), orderByItem);
            builder.append(toSQLString(statements.get(i), dbType));
            // 如果是多条sql，并且不是最后一条sql，则添加;
            if (i != statements.size() - 1 && isMultiStatement) {
                builder.append(";");
            }
        }
        return builder.toString();
    }

    /**
     * 给原始的sqlStmt添加排序项
     *
     * @param stmt        原始的sqlStmt
     * @param orderByItem 要排序的项
     **/
    public static void addOrderBy(SQLStatement stmt, SQLSelectOrderByItem orderByItem) {
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                addOrderBy(query, orderByItem);
            } else if (query instanceof SQLUnionQuery) {
                // 如果是union all 查询，则直接给order by 添加条件
                SQLOrderBy sqlOrderBy = ((SQLUnionQuery) query).getOrderBy();
                if (sqlOrderBy == null) {
                    sqlOrderBy = new SQLOrderBy();
                }
                sqlOrderBy.addItem(orderByItem);
                ((SQLUnionQuery) query).setOrderBy(sqlOrderBy);
            }
            return;
        }
        if (stmt instanceof SQLDeleteStatement) {
            throw new IllegalArgumentException("add order by  not support " + stmt.getClass().getName());
        }
        if (stmt instanceof SQLUpdateStatement) {
            throw new IllegalArgumentException("add order by  not support " + stmt.getClass().getName());
        }
        throw new IllegalArgumentException("add order by  not support " + stmt.getClass().getName());
    }

    /**
     * 明确的命令
     * 去掉sql中的order by条件
     *
     * @param sql 要去掉的sql
     * @return java.lang.String 去掉order by后的sql
     **/
    public static String clearOrderBy(String sql, DbType dbType) {
        List<SQLStatement> statements = parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(statements)) {
            throw new IllegalArgumentException("要添加sql不包含sql语句!");
        }
        SQLStatement stmt = statements.get(statements.size() - 1);
        SQLSelectQuery query;
        SQLWithSubqueryClause withSubqueryClause;
        if (stmt instanceof SQLSelectStatement) {
            withSubqueryClause = ((SQLSelectStatement) stmt).getSelect().getWithSubQuery();
            query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            clearOrderBy(query);
        } else {
            throw new IllegalArgumentException("clear order condition not support " + stmt.getClass().getName());
        }
        return (withSubqueryClause == null ? "" : SQLUtils.toSQLString(withSubqueryClause)) + "\n" + SQLUtils.toSQLString(query, dbType);
    }


    /**
     * 得到总条数的sql，从原sql中中
     *
     * @param sqlContent 要替换的sql的sql内容
     * @return 替换后的计算总条数sql
     * @throws RuntimeException 解析失败则抛出异常
     */
    public static String getTotalNumSqlFromSql(String sqlContent, DbType dbType) throws RuntimeException {
        if (StringUtils.isEmpty(sqlContent)) {
            return sqlContent;
        }
        sqlContent = SelfDruidSqlUtils.clearOrderBy(sqlContent, dbType);
        sqlContent = getLimitSqlFromSql(sqlContent, dbType, 0, Integer.MAX_VALUE);
        String returnSql;
        if (isContainGroupBy(sqlContent, dbType)) {
            returnSql = "select count(1) as " + NUM_STR + " from (" + sqlContent + ") t";
        } else {
            returnSql = SelfDruidSqlUtils.replaceSelectItem((sqlContent), " count(1) ", NUM_STR, false, dbType);
        }
        return returnSql;
    }

    /**
     * 得到limit 语句，从原sql中中
     *
     * @param sqlContent 要替换的sql的sql内容
     * @param dbType     数据库类型的枚举类
     * @param offset     偏移量
     * @param count      每页数量
     * @return 替换后的limit sql
     */
    public static String getLimitSqlFromSql(String sqlContent, com.alibaba.druid.DbType dbType, int offset, int count) {
        if (StringUtils.isEmpty(sqlContent)) {
            return sqlContent;
        }
        return PagerUtils.limit(sqlContent, dbType, offset, count);
    }

    /**
     * 删除 掉变量表达式，
     * <p>
     * ${start_tpl_var_*} xxxx ${end_tpl_var_*}
     * <p>
     * 以${start_tpl_var_*} 起始 ${end_tpl_var_*} 结尾 包含配置变量,在查询中如空或未填写,将会过滤掉包含的整段内容
     *
     * @param str 要删除的字符串
     * @return {@link String}
     */
    public static String removeVarSubRangeStr(String str) {
        // 首先找出有这些的开头和结束的标识字符
        List<String> startStrList = findSubStrPattern(str, "\\$\\{start_tpl_var_[a-zA-Z-_0-9]+\\}");
        List<String> endStrList = findSubStrPattern(str, "\\$\\{end_tpl_var_[a-zA-Z-_0-9]+\\}");
        for (int i = 0; i < Math.min(startStrList.size(), endStrList.size()); ++i) {
            str = subRangeString(str, startStrList.get(i), endStrList.get(i));
        }
        return str;
    }

    /**
     * 为查询添加 having的条件
     *
     * @param condition 条件
     * @param dbType    db型
     * @param query     查询
     */
    private static void addHavingForQuery(String condition, DbType dbType, SQLSelectQuery query) {
        SQLSelectQueryBlock sqlSelectQueryBlock = null;
        if (query instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) query;
            addHavingForQuery(condition, dbType, sqlUnionQuery.getLeft());
            addHavingForQuery(condition, dbType, sqlUnionQuery.getRight());
        } else if (query instanceof SQLSelectQueryBlock) {
            sqlSelectQueryBlock = (SQLSelectQueryBlock) query;
        } else {
            throw new IllegalArgumentException("要添加having条件的sql为非查询 union all 或普通select sql，不能添加！sql内容为：" + query);
        }
        if (sqlSelectQueryBlock == null) {
            return;
        }
        SQLSelectGroupByClause sqlSelectGroupByClause = Optional.ofNullable(sqlSelectQueryBlock.getGroupBy()).orElse(new SQLSelectGroupByClause());
        SQLExpr sqlExpr = sqlSelectGroupByClause.getHaving();
        SQLExpr newSqlExpr;
        if (sqlExpr == null) {
            newSqlExpr = SQLUtils.toSQLExpr(condition);
        } else {
            newSqlExpr = SQLUtils.toSQLExpr("(" + SQLUtils.toSQLString(sqlExpr, dbType) + " ) and (" + condition + ")");
        }
        sqlSelectGroupByClause.setHaving(newSqlExpr);
        sqlSelectQueryBlock.setGroupBy(sqlSelectGroupByClause);
    }

    /**
     * 清除查询的sql中的orderBy条件
     *
     * @param query sql查询的对象
     **/
    private static void clearOrderBy(SQLSelectQuery query) {
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
            if (queryBlock.getOrderBy() != null) {
                queryBlock.setOrderBy(null);
            }
            return;
        }

        if (query instanceof SQLUnionQuery) {
            SQLUnionQuery union = (SQLUnionQuery) query;
            if (union.getOrderBy() != null) {
                union.setOrderBy(null);
            }
            clearOrderBy(union.getLeft());
            clearOrderBy(union.getRight());
        }
    }

    /**
     * 增加group by,(sql 只支持单条的查询sql,暂不支持union all的sql)
     *
     * @param stmt             查询的stmt
     * @param groupByCondition group by 的条件
     **/
    private static void addGroupBy(SQLStatement stmt, SQLExpr groupByCondition) {
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
                SQLSelectGroupByClause groupBy = queryBlock.getGroupBy();
                if (groupBy == null) {
                    groupBy = new SQLSelectGroupByClause();
                }
                groupBy.addItem(groupByCondition);
                queryBlock.setGroupBy(groupBy);

            } else {
                throw new IllegalArgumentException("add groupBy not support " + stmt.getClass().getName());
            }
            return;
        }
        if (stmt instanceof SQLDeleteStatement) {
            throw new IllegalArgumentException("add groupBy not support " + stmt.getClass().getName());
        }
        if (stmt instanceof SQLUpdateStatement) {
            throw new IllegalArgumentException("add groupBy not support " + stmt.getClass().getName());
        }
        throw new IllegalArgumentException("add groupBy not support " + stmt.getClass().getName());
    }

    /**
     * 删除某两个子字符串范围
     *
     * @param body 要删除的字符串
     * @param str1 str1
     * @param str2 str2
     * @return {@link String}
     */
    private static String subRangeString(String body, String str1, String str2) {
        if (StringUtils.isEmpty(body)) {
            return body;
        }
        while (true) {
            int index1 = body.indexOf(str1);
            if (index1 != -1) {
                int index2 = body.indexOf(str2, index1);
                if (index2 != -1) {
                    body = body.substring(0, index1) + body.substring(index2 + str2.length());
                } else {
                    return body;
                }
            } else {
                return body;
            }
        }
    }

    /**
     * 根据表达式找出匹配的子字符串
     *
     * @param needFindString 需要找到字符串
     * @param pattern        模式
     * @return {@link List<String>}
     */
    private static List<String> findSubStrPattern(String needFindString, String pattern) {
        List<String> resultStrList = new ArrayList<>(10);
        if (StringUtils.isEmpty(needFindString)) {
            return resultStrList;
        }
        Pattern datePattern = Pattern.compile(pattern);
        Matcher dateMatcher = datePattern.matcher(needFindString);
        while (dateMatcher.find()) {
            resultStrList.add(dateMatcher.group());
        }
        return resultStrList;
    }

    /**
     * 给 SQLSelectQuery 添加where 条件
     *
     * @param query          查询
     * @param whereCondition 条件
     */
    private static void addWhereForSqlSelectQuery(SQLSelectQuery query, SQLExpr whereCondition) {
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
            queryBlock.addWhere(whereCondition);
        } else if (query instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) query;
            addWhereForSqlSelectQuery(sqlUnionQuery.getLeft(), whereCondition);
            addWhereForSqlSelectQuery(sqlUnionQuery.getRight(), whereCondition);
        } else {
            throw new IllegalArgumentException("add where not support " + query.getClass().getName());
        }
    }

    /**
     * 给selectQuery添加排序项
     *
     * @param sqlSelectQuery select的查询sql
     * @param orderByItem    要排序的项
     */
    private static void addOrderBy(SQLSelectQuery sqlSelectQuery, SQLSelectOrderByItem orderByItem) {
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            SQLOrderBy orderBy = queryBlock.getOrderBy();
            if (orderBy == null) {
                orderBy = new SQLOrderBy();
            }
            orderBy.addItem(orderByItem);
            queryBlock.setOrderBy(orderBy);
        } else {
            throw new IllegalArgumentException("add order by not support " + sqlSelectQuery.getClass().getName());
        }
    }

    /**
     * 处理条件和sql
     *
     * @param sql       sql
     * @param condition 条件 内容
     * @param dbType    db 类型
     * @return {@link TemplateBuilderSqlExpr}
     */
    private static TemplateBuilderSqlExpr handleConditionAndSql(String sql, String condition, DbType dbType) {
        if (StringUtils.isEmpty(sql) || StringUtils.isEmpty(condition)) {
            return null;
        }
        List<SQLStatement> statements = parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(statements)) {
            throw new IllegalArgumentException("要添加sql不包含sql语句!");
        }
        SQLExpr conditionExpr = toSQLExpr(condition, dbType);
        StringBuilder builder = new StringBuilder();
        boolean isMultiStatement = statements.size() > 1;
        return new TemplateBuilderSqlExpr(statements, conditionExpr, builder, isMultiStatement);
    }


    /**
     * 获得sql 真正的查询别名，例如 a as '中国'；解析到别名为：'中国'，然后可以调用该方法，获取到中国
     *
     * @param alias 别名
     * @return {@link String}
     */
    public static String getRealAlias(String alias) {
        if (alias == null || alias.length() == 0) {
            return alias;
        }
        char first = alias.charAt(0);
        if (first == '"' || first == '\'' || first == '`') {
            char[] chars = new char[alias.length() - 2];
            int len = 0;
            for (int i = 1; i < alias.length() - 1; ++i) {
                char ch = alias.charAt(i);
                if (ch == '\\') {
                    ++i;
                    ch = alias.charAt(i);
                }
                chars[len++] = ch;
            }
            return new String(chars, 0, len);
        }

        return alias;
    }

    /**
     * 临时的 用来存放变量的内部类
     *
     * @author zhangcanlong
     * @date 2021/10/21
     */
    @Data
    @AllArgsConstructor
    static class TemplateBuilderSqlExpr {
        private List<SQLStatement> statements;
        private SQLExpr conditionExpr;
        private StringBuilder builder;
        private boolean multiStatement;
    }

}
