package com.alibaba.datax.plugin.writer.gpdbwriter;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.Key;

/**
 * Greenplum COPY Writer 插件入口类
 * 
 * 使用 PostgreSQL COPY 协议实现高效的批量写入。
 * Job 层复用 CommonRdbmsWriter.Job（通过 GreenplumCopyWriterJob 继承扩展）。
 * Task 层采用 Pipeline 多线程架构（CopyWriterTask → CopyProcessor → CopyWorker）。
 */
public class GreenplumWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;
        private GreenplumCopyWriterJob copyJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            // Greenplum 仅支持 COPY 模式，不允许配置 writeMode
            String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
            if (null != writeMode) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        String.format("写入模式(writeMode)配置有误. Greenplum 仅支持 COPY 模式写入，"
                                + "不支持配置 writeMode: %s. 请删除该配置项.", writeMode));
            }

            // 校验 segment_reject_limit
            int segmentRejectLimit = this.originalConfig.getInt("segment_reject_limit", 0);
            if (segmentRejectLimit != 0 && segmentRejectLimit < 2) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "segment_reject_limit 必须为 0（不启用）或 >= 2");
            }

            this.copyJob = new GreenplumCopyWriterJob();
            this.copyJob.init(this.originalConfig);
        }

        @Override
        public void prepare() {
            this.copyJob.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.copyJob.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.copyJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.copyJob.destroy(this.originalConfig);
        }
    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CopyWriterTask copyTask;

        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.copyTask = new CopyWriterTask();
            this.copyTask.init(this.writerSliceConfig);
        }

        @Override
        public void prepare() {
            this.copyTask.prepare(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            this.copyTask.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post() {
            this.copyTask.post(this.writerSliceConfig);
        }

        @Override
        public void destroy() {
            this.copyTask.destroy(this.writerSliceConfig);
        }
    }
}
