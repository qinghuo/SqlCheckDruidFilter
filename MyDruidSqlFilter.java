/**
 *
 * Copyright (C), 2011-2018, 微贷网.
 */
package com.zeng.filter;

import com.alibaba.druid.filter.FilterChain;
import com.alibaba.druid.filter.FilterEventAdapter;
import com.alibaba.druid.proxy.jdbc.ConnectionProxy;
import com.alibaba.druid.proxy.jdbc.PreparedStatementProxy;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.weidai.ums.common.exception.UmsBusinessException;
import lombok.Data;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author zengzhenmin 2018/8/24.
 */
@Data
public class MyDruidSqlFilter extends FilterEventAdapter {
    private static org.slf4j.Logger LOG = LoggerFactory.getLogger(MyDruidSqlFilter.class);
    private String configFilePath;
    private Map<String/** tableName **/
            , Set<String>/** table fields **/
    > tableName2Fields;
    private Integer defaultLimit = 200;
    private boolean failThrowException = false;
    private Boolean addLimit = true;
    final String dbType = JdbcConstants.MYSQL; // 可以是ORACLE、POSTGRESQL、SQLSERVER、ODPS等

    public MyDruidSqlFilter(String configFilePath) {
        this.configFilePath = configFilePath;
        try {
            PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = pathMatchingResourcePatternResolver.getResources(configFilePath);
            YamlReader reader = new YamlReader(new InputStreamReader(resources[0].getInputStream()));
            SqlCheckConfig sqlCheckConfig = reader.read(SqlCheckConfig.class);
            tableName2Fields = new HashedMap();
            List<TableConfig> tableConfigs = sqlCheckConfig.getTableConfigs();
            for (TableConfig tableConfig : tableConfigs) {
                tableName2Fields.put(tableConfig.getTableName(), tableConfig.getFeatureFields());
            }
            if (null != sqlCheckConfig.getDefaultLimit()) {
                defaultLimit = sqlCheckConfig.getDefaultLimit();
            }
            if (null != sqlCheckConfig.getFailThrowException()) {
                failThrowException = sqlCheckConfig.getFailThrowException();
            }
            if (null != sqlCheckConfig.getAddLimit()) {
                addLimit = sqlCheckConfig.getAddLimit();
            }
        } catch (IOException e) {
            LOG.error("MyDruidSqlFilter 初始化失败", e);
            throw new MyBusinessException("UmsDruidSqlFilter 初始化错误");
        }
    }

    @Override
    public PreparedStatementProxy connection_prepareStatement(FilterChain chain, ConnectionProxy connection, String sql) throws SQLException {
        CheckResult checkResult;
        try {
            checkResult = checkSql(sql);
        } catch (Exception e) {
            LOG.error("MyDruidSqlFilter sql解析失败" + sql, e);
            checkResult = CheckResult.SUCCESS;
        }
        if (CheckResult.NEED_ADD_LIMIT == checkResult) {
            LOG.error("MyDruidSqlFilter sql illegal! :" + sql);
            sql = sql + " limit " + defaultLimit;
        } else if (CheckResult.FAIL == checkResult) {
            LOG.error("MyDruidSqlFilter sql illegal! :" + sql);
            if (failThrowException) {
                throw new RuntimeException("sql result maybe size too large!");
            }
        }
        return chain.connection_prepareStatement(connection, sql);
    }

    private CheckResult checkSql(String sql) {
        CheckResult checkResult = CheckResult.SUCCESS;
        if (StringUtils.isBlank(sql)) {
            return checkResult;
        }
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        if (CollectionUtils.isEmpty(stmtList)) {
            return checkResult;
        }
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof SQLSelectStatement) {
                if (!doCheckSql(stmt, sql)) {
                    checkResult = CheckResult.FAIL;
                    break;
                }
            }
        }
        if (addLimit && CheckResult.FAIL == checkResult && stmtList.size() == 1 && !sql.contains("limit")) {
            return CheckResult.NEED_ADD_LIMIT;
        }
        return checkResult;
    }

    private boolean doCheckSql(SQLStatement stmt, String sql) {
        MySqlSchemaStatVisitor schemaStatVisitor = new MySqlSchemaStatVisitor();
        LimitVisitor limitVisitor = new LimitVisitor();
        stmt.accept(schemaStatVisitor);
        stmt.accept(limitVisitor);
        if (limitVisitor.getHasLimit()) {
            return true;
        }
        List<TableStat.Condition> conditions = schemaStatVisitor.getConditions();
        Set<TableStat.Name> tableNames = schemaStatVisitor.getTables().keySet();
        for (TableStat.Name name : tableNames) {
            if (!tableName2Fields.containsKey(name.getName())) {
                return true;
            }
        }
        for (TableStat.Condition condition : conditions) {
            if (tableName2Fields.get(condition.getColumn().getTable()).contains(condition.getColumn().getName())) {
                return true;
            }
        }
        return false;
    }

    @Data
    static class LimitVisitor extends MySqlASTVisitorAdapter {
        private Boolean hasLimit = false;

        @Override
        public boolean visit(SQLLimit x) {
            hasLimit = true;
            return true;
        }
    }

    @Data
    public static class SqlCheckConfig {
        private Integer defaultLimit;
        private Boolean failThrowException;
        private Boolean addLimit;
        private List<TableConfig> tableConfigs = new ArrayList<>();
    }

    @Data
    public static class TableConfig {
        private String tableName;
        private Set<String> featureFields = new HashSet<>();
    }

    public enum CheckResult {
        NEED_ADD_LIMIT(0), SUCCESS(1), FAIL(2);
        private Integer code;

        CheckResult(Integer code) {
            this.code = code;
        }
    }

    public static void main(String[] args) throws ExecutionException {
        final String dbType = JdbcConstants.MYSQL; //
        String sql = "SELECT * FROM u_base ub left join u_base_extend ube on ube.uid=ub.id WHERE ub.id=? and ube.uid=? limit 10";
        String sql1 = "SELECT * FROM article WHERE uid = (SELECT uid FROM user WHERE status=1 ORDER BY uid DESC LIMIT 1)\n";
        String sql2 = "select *  from u_base where id < 100 and id >3 limit ?";
        String sql3 = "select * from u_base where uid BETWEEN 3 and 5";
        String sql4 = "select * from u_base where uid in (3 )";
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        MySqlSchemaStatVisitor schemaStatVisitor = new MySqlSchemaStatVisitor();
        LimitVisitor limitVisitor = new LimitVisitor();
        for (SQLStatement stmt : stmtList) {
            stmt.accept(schemaStatVisitor);
            stmt.accept(limitVisitor);
        }
        System.out.println("");
    }

}
