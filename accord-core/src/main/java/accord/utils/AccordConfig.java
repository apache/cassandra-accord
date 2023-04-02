package accord.utils;

public class AccordConfig {
    public long progress_log_scheduler_delay_in_ms = 200L;

    public AccordConfig(long progress_log_scheduler_delay_in_ms) {
        this.progress_log_scheduler_delay_in_ms = progress_log_scheduler_delay_in_ms;
    }

    public AccordConfig() {}
}
