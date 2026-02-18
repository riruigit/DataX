package com.alibaba.datax.plugin.writer.gpdbwriter;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

/**
 * COPY 写入工作线程
 * 
 * 从 data 队列消费 byte[] 数据，通过 PipedOutputStream → PipedInputStream 管道
 * 喂给 CopyManager.copyIn()，实现流式 COPY 写入 Greenplum。
 * 
 * 核心机制:
 * 1. 构造时创建独立的数据库连接和 Pipe 管道
 * 2. 启动后台线程执行 CopyManager.copyIn(sql, pipeIn)
 * 3. call() 方法循环从队列读取 byte[] 写入 pipeOut
 * 4. 所有数据处理完毕后关闭管道，后台线程自动结束
 * 5. 异常时取消 COPY 查询并中断后台线程
 */
public class CopyWorker implements Callable<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyWorker.class);

    private CopyWriterTask task = null;
    private Connection connection;
    private LinkedBlockingQueue<byte[]> queue = null;
    private FutureTask<Long> copyResult = null;
    private String sql = null;
    private PipedInputStream pipeIn = null;
    private PipedOutputStream pipeOut = null;
    private Thread copyBackendThread = null;

    public CopyWorker(CopyWriterTask task, String copySql,
                      LinkedBlockingQueue<byte[]> queue) throws IOException {
        this.task = task;
        this.connection = task.createConnection();
        this.queue = queue;
        this.pipeOut = new PipedOutputStream();
        this.pipeIn = new PipedInputStream(pipeOut);
        this.sql = copySql;

        // 如果配置了较大的 CSV 行限制，设置 GP 参数
        if (task.getMaxCsvLineSize() >= 1024) {
            changeCsvSizeLimit(connection);
        }

        // 启动后台线程执行 CopyManager.copyIn()
        this.copyResult = new FutureTask<Long>(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                try {
                    CopyManager mgr = new CopyManager((BaseConnection) connection);
                    return mgr.copyIn(sql, pipeIn);
                } finally {
                    try {
                        pipeIn.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        });

        copyBackendThread = new Thread(copyResult);
        copyBackendThread.setName("CopyManager-Backend");
        copyBackendThread.setDaemon(true);
        copyBackendThread.start();
    }

    @Override
    public Long call() throws Exception {
        Thread.currentThread().setName("CopyWorker");

        byte[] data = null;
        try {
            while (true) {
                data = queue.poll(1000L, TimeUnit.MILLISECONDS);

                // 所有 Processor 已完成且队列为空, 退出
                if (data == null && !task.moreData()) {
                    break;
                } else if (data == null) {
                    continue;
                }

                pipeOut.write(data);
            }

            pipeOut.flush();
            pipeOut.close();
        } catch (Exception e) {
            // 异常时取消 COPY 查询
            try {
                ((BaseConnection) connection).cancelQuery();
            } catch (SQLException ignore) {
                // 取消查询失败, 忽略
            }

            // 中断后台线程
            try {
                copyBackendThread.interrupt();
            } catch (SecurityException ignore) {
            }

            // 尝试获取 CopyManager 的原始错误信息
            try {
                copyResult.get();
            } catch (ExecutionException exec) {
                if (exec.getCause() instanceof PSQLException) {
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.WRITE_DATA_ERROR, exec.getCause());
                }
                // 其他异常忽略
            } catch (Exception ignore) {
            }

            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            try {
                pipeOut.close();
            } catch (Exception e) {
                // 关闭管道失败, 忽略
            }

            try {
                copyBackendThread.join(0);
            } catch (Exception e) {
                // 线程被中断, 忽略
            }

            DBUtil.closeDBResources(null, null, connection);
        }

        // 获取写入行数
        try {
            Long count = copyResult.get();
            LOG.info("CopyWorker 完成, 写入 {} 行", count);
            return count;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    /**
     * 设置 Greenplum 的 gp_max_csv_line_length 参数
     * 允许 COPY 操作接受更大的 CSV 行
     */
    private void changeCsvSizeLimit(Connection conn) {
        List<String> sqls = new ArrayList<String>();
        sqls.add("SET gp_max_csv_line_length = " + Integer.toString(task.getMaxCsvLineSize()));

        try {
            WriterUtil.executeSqls(conn, sqls, task.getJdbcUrl(), DataBaseType.Greenplum);
        } catch (Exception e) {
            LOG.warn("无法设置 gp_max_csv_line_length 为 {}", task.getMaxCsvLineSize());
        }
    }
}
