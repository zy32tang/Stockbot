package com.stockbot.app.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "mail")
public class MailProperties {
    private Boolean dryRun;
    private Boolean failFast;
    private String dryRunDir;
}

