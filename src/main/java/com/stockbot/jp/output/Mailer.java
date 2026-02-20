package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 模块说明：Mailer（class）。
 * 主要职责：承载 output 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class Mailer {

    public static final class Settings {
        public boolean enabled;
        public String host;
        public int port;
        public String user;
        public String pass;
        public String from;
        public List<String> to;
        public String subjectPrefix;
    }

/**
 * 方法说明：loadSettings，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Settings loadSettings(Config config) {
        Settings settings = new Settings();
        settings.enabled = config.getBoolean("email.enabled", true);
        settings.host = config.getString("email.smtp_host", "smtp.gmail.com");
        settings.port = config.getInt("email.smtp_port", 587);
        settings.user = config.getString("email.smtp_user", "");
        settings.pass = config.getString("email.smtp_pass", "");
        settings.from = config.getString("email.from", settings.user);
        settings.to = config.getList("email.to");
        settings.subjectPrefix = config.getString("email.subject_prefix", "[StockBot JP]");
        return settings;
    }

/**
 * 方法说明：send，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void send(Settings s, String subject, String textBody, String htmlBody) throws MessagingException {
        send(s, subject, textBody, htmlBody, List.of());
    }

/**
 * 方法说明：send，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void send(Settings s, String subject, String textBody, String htmlBody, List<Path> attachments) throws MessagingException {
        if (!s.enabled) {
            return;
        }
        if (s.host.isEmpty() || s.user.isEmpty() || s.pass.isEmpty() || s.to.isEmpty()) {
            throw new IllegalArgumentException("email enabled but smtp settings are incomplete");
        }

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", s.host);
        props.put("mail.smtp.port", String.valueOf(s.port));

        Session session = Session.getInstance(props, new Authenticator() {
/**
 * 方法说明：getPasswordAuthentication，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(s.user, s.pass);
            }
        });

        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(s.from));
        String toJoined = s.to.stream().map(String::trim).filter(v -> !v.isEmpty()).collect(Collectors.joining(","));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toJoined));
        message.setSubject(subject, "UTF-8");

        boolean hasHtml = htmlBody != null && !htmlBody.trim().isEmpty();
        String safeText = textBody == null ? "" : textBody;
        List<Path> safeAttachments = attachments == null ? List.of() : new ArrayList<>(attachments);
        if (safeAttachments.isEmpty()) {
            if (hasHtml) {
                message.setContent(htmlBody, "text/html; charset=UTF-8");
            } else {
                message.setText(safeText, "UTF-8");
            }
            Transport.send(message);
            return;
        }

        MimeMultipart mixed = new MimeMultipart("mixed");
        MimeBodyPart bodyPart = new MimeBodyPart();
        if (hasHtml) {
            bodyPart.setContent(htmlBody, "text/html; charset=UTF-8");
        } else {
            bodyPart.setText(safeText, "UTF-8");
        }
        mixed.addBodyPart(bodyPart);

        for (Path attachment : safeAttachments) {
            if (attachment == null) {
                continue;
            }
            try {
                MimeBodyPart filePart = new MimeBodyPart();
                filePart.attachFile(attachment.toFile());
                mixed.addBodyPart(filePart);
            } catch (Exception ignored) {
                // best effort; keep sending mail even if one attachment fails
            }
        }

        message.setContent(mixed);
        Transport.send(message);
    }
}
