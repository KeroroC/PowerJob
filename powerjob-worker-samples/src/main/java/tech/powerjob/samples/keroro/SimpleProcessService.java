package tech.powerjob.samples.keroro;

import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import tech.powerjob.worker.annotation.PowerJobHandler;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.log.OmsLogger;

/**
 * @author wangpeng
 * @date 2023年09月13日 20:16
 */
@Service
public class SimpleProcessService {

    @PowerJobHandler(name = "task1")
    public String task1(TaskContext context) {
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("没毛病");

        return "task1 succeed";
    }

    @PowerJobHandler(name = "task2")
    public String task2(TaskContext context) {
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("okk");

        return "task2 succeed";
    }

    @PowerJobHandler(name = "task3")
    public String task3(TaskContext context) {
        OmsLogger omsLogger = context.getOmsLogger();
        omsLogger.info("okk");

        return "task3 succeed";
    }
}
