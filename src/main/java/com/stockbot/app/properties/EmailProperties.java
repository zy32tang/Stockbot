package com.stockbot.app.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "email")
public class EmailProperties {
    private boolean enabled = true;
    private String smtpHost = "smtp.gmail.com";
    private int smtpPort = 587;
    private String smtpUser = "";
    private String smtpPass = "";
    private String from = "";
    private List<String> to = new ArrayList<>();
    private String subjectPrefix = "[StockBot JP]";
}

