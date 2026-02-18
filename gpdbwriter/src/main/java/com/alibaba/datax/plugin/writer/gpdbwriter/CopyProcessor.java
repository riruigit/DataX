package com.alibaba.datax.plugin.writer.gpdbwriter;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;

/**
 * COPY 数据序列化处理器
 * 
 * 从 Record 队列消费，将 DataX Record 序列化为 CSV 格式的 byte[]，
 * 然后放入 data 队列供 CopyWorker 消费。
 * 
 * CSV 格式规则:
 * - 字段分隔符: '|'
 * - 引号字符: '"'（仅字符串类型使用）
 * - 转义字符: '\'
 * - NULL: 空字段
 * - 行分隔符: '\n'
 * 
 * 类型感知序列化:
 * - CHAR/VARCHAR 等字符串类型: 引号包裹 + escapeString (处理引号和反斜杠转义, 过滤 0x00)
 * - BINARY/BLOB 等二进制类型: 八进制转义 (escapeBinary)  
 * - 其他类型 (数值/日期/布尔): 直接 asString()
 */
public class CopyProcessor implements Callable<Long> {
    private static final char FIELD_DELIMITER = '|';
    private static final char NEWLINE = '\n';
    private static final char QUOTE = '"';
    private static final char ESCAPE = '\\';
    private static final int MAX_CSV_SIZE = 4 * 1024 * 1024; // 4MB 超大行保护

    private static final Logger LOG = LoggerFactory.getLogger(CopyProcessor.class);

    private int columnNumber;
    private CopyWriterTask task;
    private LinkedBlockingQueue<Record> queueIn;
    private LinkedBlockingQueue<byte[]> queueOut;
    private Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    public CopyProcessor(CopyWriterTask task, int columnNumber,
                         Triple<List<String>, List<Integer>, List<String>> resultSetMetaData,
                         LinkedBlockingQueue<Record> queueIn,
                         LinkedBlockingQueue<byte[]> queueOut) {
        this.task = task;
        this.columnNumber = columnNumber;
        this.resultSetMetaData = resultSetMetaData;
        this.queueIn = queueIn;
        this.queueOut = queueOut;
    }

    @Override
    public Long call() throws Exception {
        Thread.currentThread().setName("CopyProcessor");
        Record record = null;

        while (true) {
            record = queueIn.poll(1000L, TimeUnit.MILLISECONDS);

            // 主线程已结束且队列为空, 退出
            if (record == null && !task.moreRecord()) {
                break;
            } else if (record == null) {
                continue; // 队列暂时为空, 继续等待
            }

            // 列数校验
            if (record.getColumnNumber() != this.columnNumber) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 "
                                + "目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                                record.getColumnNumber(), this.columnNumber));
            }

            byte[] data = serializeRecord(record);

            // 超大行保护: 超过 4MB 的行丢弃并告警
            if (data.length > MAX_CSV_SIZE) {
                String preview = new String(data, 0, Math.min(100, data.length));
                LOG.warn("数据行超过 {}B 限制被忽略: {}...", MAX_CSV_SIZE, preview);
            } else {
                queueOut.put(data);
            }
        }

        return 0L;
    }

    /**
     * 将 Record 序列化为 CSV 格式的 byte[]
     * 
     * 按照 SQL 类型分类处理:
     * - 字符串类型 (CHAR/VARCHAR/NCHAR/NVARCHAR/LONGVARCHAR/LONGNVARCHAR): 引号包裹 + 转义
     * - 二进制类型 (BINARY/BLOB/CLOB/LONGVARBINARY/NCLOB/VARBINARY): 八进制转义
     * - 其他类型 (数值/日期/布尔等): 直接 asString()
     * - NULL 值: 空字段（不输出任何内容）
     */
    protected byte[] serializeRecord(Record record) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        Column column;

        for (int i = 0; i < this.columnNumber; i++) {
            column = record.getColumn(i);
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);

            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    // 字符串类型: 引号包裹 + 转义特殊字符
                    String data = column.asString();
                    if (data != null) {
                        sb.append(QUOTE);
                        sb.append(escapeString(data));
                        sb.append(QUOTE);
                    }
                    break;
                }
                case Types.BINARY:
                case Types.BLOB:
                case Types.CLOB:
                case Types.LONGVARBINARY:
                case Types.NCLOB:
                case Types.VARBINARY: {
                    // 二进制类型: 八进制转义
                    byte[] data = column.asBytes();
                    if (data != null) {
                        sb.append(escapeBinary(data));
                    }
                    break;
                }
                default: {
                    // 数值/日期/布尔等: 直接输出
                    String data = column.asString();
                    if (data != null) {
                        sb.append(data);
                    }
                    break;
                }
            }

            // 字段间用分隔符分开
            if (i + 1 < this.columnNumber) {
                sb.append(FIELD_DELIMITER);
            }
        }
        sb.append(NEWLINE);
        return sb.toString().getBytes("UTF-8");
    }

    /**
     * CSV 字符串转义
     * 
     * - QUOTE(") 和 ESCAPE(\) 字符前面加转义符
     * - 过滤非法的 0x00 空字节（PostgreSQL COPY 不接受）
     */
    protected String escapeString(String data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length(); ++i) {
            char c = data.charAt(i);
            switch (c) {
                case 0x00:
                    LOG.warn("字符串中发现非法字符 0x00，已将其删除");
                    continue;
                case QUOTE:
                case ESCAPE:
                    sb.append(ESCAPE);
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * 二进制数据八进制转义
     * 
     * - 反斜杠 '\' → '\\\\'
     * - 非打印字符 (< 0x20 或 > 0x7e) → '\\nnn' (八进制表示)
     * - 其他可打印字符: 原样输出
     */
    protected String escapeBinary(byte[] data) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < data.length; ++i) {
            if (data[i] == '\\') {
                sb.append('\\');
                sb.append('\\');
            } else if (data[i] < 0x20 || data[i] > 0x7e) {
                byte b = data[i];
                char[] val = new char[3];
                val[2] = (char) ((b & 07) + '0');
                b >>= 3;
                val[1] = (char) ((b & 07) + '0');
                b >>= 3;
                val[0] = (char) ((b & 03) + '0');
                sb.append('\\');
                sb.append(val);
            } else {
                sb.append((char) (data[i]));
            }
        }

        return sb.toString();
    }
}
