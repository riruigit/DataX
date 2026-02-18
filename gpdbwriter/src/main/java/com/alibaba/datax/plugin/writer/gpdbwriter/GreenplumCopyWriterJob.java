package com.alibaba.datax.plugin.writer.gpdbwriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

/**
 * Greenplum COPY Writer Job 层
 * 
 * 继承 CommonRdbmsWriter.Job，复用 preSql/postSql/split 等通用逻辑，
 * 扩展 Greenplum 特有的 error log 管理功能：
 * - prepare 阶段：清理各表的 error log（gp_truncate_error_log）
 * - post 阶段：读取各表的 error log 统计并输出警告（gp_read_error_log）
 */
public class GreenplumCopyWriterJob extends CommonRdbmsWriter.Job {
    private static final Logger LOG = LoggerFactory.getLogger(GreenplumCopyWriterJob.class);
    private List<String> tables = null;

    public GreenplumCopyWriterJob() {
        super(DataBaseType.Greenplum);
    }

    @Override
    public void init(Configuration originalConfig) {
        super.init(originalConfig);
    }

    @Override
    public void prepare(Configuration originalConfig) {
        // 获取表列表，清理 GP error log
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);

        List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
        Configuration connConf = Configuration.from(conns.get(0).toString());

        String jdbcUrl = connConf.getString(Key.JDBC_URL);
        tables = connConf.getList(Key.TABLE, String.class);

        Connection conn = DBUtil.getConnection(DataBaseType.Greenplum, jdbcUrl, username, password);

        List<String> sqls = new ArrayList<String>();
        for (String table : tables) {
            sqls.add("SELECT gp_truncate_error_log('" + table + "');");
            LOG.info("为 {} 清理 ERROR LOG. context info:{}.", table, jdbcUrl);
        }

        WriterUtil.executeSqls(conn, sqls, jdbcUrl, DataBaseType.Greenplum);
        DBUtil.closeDBResources(null, null, conn);

        // 执行 preSql
        super.prepare(originalConfig);
    }

    @Override
    public void post(Configuration originalConfig) {
        super.post(originalConfig);

        // 读取 error log，输出写入错误统计
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

        Connection conn = DBUtil.getConnection(DataBaseType.Greenplum, jdbcUrl, username, password);

        for (String table : tables) {
            int errors = 0;
            ResultSet res = null;
            String sql = "SELECT count(*) FROM gp_read_error_log('" + table + "');";

            try {
                res = DBUtil.query(conn, sql, 10);
                if (res.next()) {
                    errors = res.getInt(1);
                }
                res.close();
                conn.commit();
            } catch (SQLException e) {
                LOG.debug("获取 error log 信息失败: " + e.getMessage());
            }

            if (errors > 0) {
                LOG.warn("加载表 {} 时发现 {} 条错误数据, 使用 \"SELECT * FROM gp_read_error_log('{}');\" 查看详情",
                        table, errors, table);
            }
        }

        DBUtil.closeDBResources(null, null, conn);
    }
}
