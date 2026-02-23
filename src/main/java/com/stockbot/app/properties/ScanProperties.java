package com.stockbot.app.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "scan")
public class ScanProperties {
    private int threads = 3;
    private int topN = 15;
    private int marketReferenceTopN = 5;
    private Batch batch = new Batch();

    @Getter
    @Setter
    public static class Batch {
        private boolean enabled = true;
        private boolean resumeEnabled = true;
        private String checkpointKey = "daily.scan.batch.checkpoint.v1";
    }
}

