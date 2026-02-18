package com.alibaba.datax.plugin.writer.gpdbwriter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

/**
 * COPY 写入 Task 层 — Pipeline 编排
 * 
 * 继承 CommonRdbmsWriter.Task 以复用 protected 字段（username/password/jdbcUrl/table/columns 等），
 * 但 override startWrite() 实现 Pipeline 多线程 COPY 写入，绕过父类的 PreparedStatement insert 逻辑。
 * 
 * Pipeline 架构:
 *   主线程(RecordReceiver) → LinkedBlockingQueue<Record> → CopyProcessor×N(序列化) 
 *   → LinkedBlockingQueue<byte[]> → CopyWorker×M(COPY写入) → Greenplum
 */
public class CopyWriterTask extends CommonRdbmsWriter.Task {
    private static final Logger LOG = LoggerFactory.getLogger(CopyWriterTask.class);

    private Configuration writerSliceConfig = null;
    private int numProcessor;
    private int numWriter;
    private int queueSize;
    private int maxCsvLineSize;

    // 通过 volatile 标志位控制生产者-消费者停机
    private volatile boolean stopProcessor = false;
    private volatile boolean stopWriter = false;

    private CompletionService<Long> cs = null;

    public CopyWriterTask() {
        super(DataBaseType.Greenplum);
    }

    // --- CopyWorker 和 CopyProcessor 需要访问的方法 ---

    public String getJdbcUrl() {
        return this.jdbcUrl;
    }

    public int getMaxCsvLineSize() {
        return maxCsvLineSize;
    }

    /**
     * CopyProcessor 轮询此方法判断是否还有更多 Record
     * 当主线程读完所有 Record 后设为 false
     */
    public boolean moreRecord() {
        return !stopProcessor;
    }

    /**
     * CopyWorker 轮询此方法判断是否还有更多 byte[] 数据
     * 当所有 CopyProcessor 完成后设为 false
     */
    public boolean moreData() {
        return !stopWriter;
    }

    /**
     * 创建新的数据库连接，CopyWorker 使用独立连接执行 COPY
     */
    public Connection createConnection() {
        Connection connection = DBUtil.getConnection(
                this.dataBaseType, this.jdbcUrl, username, password);
        DBUtil.dealWithSessionConfig(connection, writerSliceConfig,
                this.dataBaseType, BASIC_MESSAGE);
        return connection;
    }

    /**
     * 构建列名列表，每个列名用双引号包裹，避免大小写敏感和保留字冲突
     */
    private String constructColumnNameList(List<String> columnList) {
        List<String> columns = new ArrayList<String>();
        for (String column : columnList) {
            if (column.endsWith("\"") && column.startsWith("\"")) {
                columns.add(column);
            } else {
                columns.add("\"" + column + "\"");
            }
        }
        return StringUtils.join(columns, ",");
    }

    /**
     * 构建 COPY SQL
     * 
     * 格式: COPY table("col1","col2") FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '"' ESCAPE E'\\\\'
     * 可选: LOG ERRORS SEGMENT REJECT LIMIT N（GP 容错机制）
     */
    public String getCopySql(String tableName, List<String> columnList,
                             int segmentRejectLimit) {
        StringBuilder sb = new StringBuilder()
                .append("COPY ").append(tableName)
                .append("(").append(constructColumnNameList(columnList)).append(")")
                .append(" FROM STDIN WITH DELIMITER '|' NULL '' CSV QUOTE '\"' ESCAPE E'\\\\'");

        if (segmentRejectLimit >= 2) {
            sb.append(" LOG ERRORS SEGMENT REJECT LIMIT ").append(segmentRejectLimit);
        }
        sb.append(";");
        return sb.toString();
    }

    /**
     * 带超时的入队操作，同时检查子线程是否异常退出
     */
    private void send(Record record, LinkedBlockingQueue<Record> queue)
            throws InterruptedException, ExecutionException {
        while (!queue.offer(record, 1000, TimeUnit.MILLISECONDS)) {
            LOG.debug("Record 队列已满, 可适当增大 num_copy_processor 提升性能.");
            Future<Long> result = cs.poll();
            if (result != null) {
                result.get(); // 如果子线程异常，这里会抛出 ExecutionException
            }
        }
    }

    /**
     * 核心方法: Pipeline 多线程 COPY 写入
     * 
     * 流程:
     * 1. 读取 Pipeline 配置参数
     * 2. 获取列元数据用于类型感知序列化
     * 3. 启动 N 个 CopyProcessor + M 个 CopyWorker
     * 4. 主线程从 RecordReceiver 读取 Record 放入 recordQueue
     * 5. 等待所有 Processor 和 Worker 完成
     */
    @Override
    public void startWrite(RecordReceiver recordReceiver,
                           Configuration writerSliceConfig,
                           TaskPluginCollector taskPluginCollector) {
        this.writerSliceConfig = writerSliceConfig;

        // 读取 Pipeline 配置参数
        int segmentRejectLimit = writerSliceConfig.getInt("segment_reject_limit", 0);
        this.queueSize = Math.max(writerSliceConfig.getInt("copy_queue_size", 1000), 10);
        this.numProcessor = Math.max(writerSliceConfig.getInt("num_copy_processor", 4), 1);
        this.numWriter = Math.max(writerSliceConfig.getInt("num_copy_writer", 1), 1);
        this.maxCsvLineSize = writerSliceConfig.getInt("max_csv_line_size", 0);

        String sql = getCopySql(this.table, this.columns, segmentRejectLimit);
        LOG.info("COPY SQL: {}", sql);

        // 两级队列：Record 队列 和 byte[] 队列
        LinkedBlockingQueue<Record> recordQueue = new LinkedBlockingQueue<Record>(queueSize);
        LinkedBlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<byte[]>(queueSize);

        // 创建线程池
        ExecutorService threadPool = Executors.newFixedThreadPool(numProcessor + numWriter);
        cs = new ExecutorCompletionService<Long>(threadPool);

        // 获取列元数据用于类型感知序列化
        Connection connection = createConnection();

        try {
            this.resultSetMetaData = DBUtil.getColumnMetaData(
                    connection, this.table, constructColumnNameList(this.columns));

            // 1. 启动 N 个 CopyProcessor（序列化线程）
            for (int i = 0; i < numProcessor; i++) {
                cs.submit(new CopyProcessor(this, this.columnNumber,
                        resultSetMetaData, recordQueue, dataQueue));
            }

            // 2. 启动 M 个 CopyWorker（COPY 写入线程）
            for (int i = 0; i < numWriter; i++) {
                cs.submit(new CopyWorker(this, sql, dataQueue));
            }

            // 3. 主线程：读取 Record 放入队列
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                send(record, recordQueue);

                // 顺便检查子线程是否有异常
                Future<Long> result = cs.poll();
                if (result != null) {
                    result.get();
                }
            }

            // 4. 通知 Processor 停止，并等待全部完成
            stopProcessor = true;
            for (int i = 0; i < numProcessor; i++) {
                cs.take().get();
            }

            // 5. 通知 Writer 停止，并等待全部完成
            stopWriter = true;
            for (int i = 0; i < numWriter; i++) {
                cs.take().get();
            }

            LOG.info("Greenplum COPY 写入完成");
        } catch (ExecutionException e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e.getCause());
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            threadPool.shutdownNow();
            DBUtil.closeDBResources(null, null, connection);
        }
    }
}
