package tech.powerjob.samples.keroro;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tech.powerjob.worker.annotation.PowerJobHandler;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import tech.powerjob.worker.log.OmsLogger;

/**
 * @author wangpeng
 * @date 2023年09月13日 16:53
 */
@Slf4j
@Component
public class StandaloneProcessor implements BasicProcessor {
    @Override
    public ProcessResult process(TaskContext context) throws Exception {
        // PowerJob 在线日志功能，使用该 Logger 打印的日志可以直接在 PowerJob 控制台查看
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("单机处理器Demo start process,context is {}.", context);

        return new ProcessResult(true, "process successfully~");
    }
}
