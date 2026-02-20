package com.stockbot.output;

import com.stockbot.utils.TextFormatter;

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * 模块说明：EmailSender（class）。
 * 主要职责：承载 output 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class EmailSender {

    public static class EmailSettings {
        public String smtpHost;
        public int smtpPort;
        public String smtpUser;
        public String smtpPass;
        public String to;
        public String from;
    }

/**
 * 方法说明：send，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void send(EmailSettings s, String subject, String body) throws MessagingException {
        send(s, subject, body, null, List.of());
    }

/**
 * 方法说明：send，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void send(EmailSettings s, String subject, String textBody, String htmlBody, List<Path> attachments)
            throws MessagingException {
        try {
            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", s.smtpHost);
            props.put("mail.smtp.port", String.valueOf(s.smtpPort));

            Session session = Session.getInstance(props, new Authenticator() {
/**
 * 方法说明：getPasswordAuthentication，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(s.smtpUser, s.smtpPass);
                }
            });

            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(s.from));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(s.to));
            message.setSubject(subject, "UTF-8");

            MimeMultipart mixed = new MimeMultipart("mixed");
            MimeBodyPart contentPart = new MimeBodyPart();
            MimeMultipart alt = new MimeMultipart("alternative");

            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(TextFormatter.cleanForEmail(textBody == null ? "" : textBody), "UTF-8");
            alt.addBodyPart(textPart);

            if (htmlBody != null && !htmlBody.trim().isEmpty()) {
                MimeBodyPart htmlPart = new MimeBodyPart();
                htmlPart.setContent(htmlBody, "text/html; charset=UTF-8");
                alt.addBodyPart(htmlPart);
            }

            contentPart.setContent(alt);
            mixed.addBodyPart(contentPart);

            if (attachments != null) {
                for (Path p : attachments) {
                    if (p == null) continue;
                    MimeBodyPart att = new MimeBodyPart();
                    att.attachFile(p.toFile());
                    mixed.addBodyPart(att);
                }
            }

            message.setContent(mixed);
            Transport.send(message);
        } catch (MessagingException e) {
            throw e;
        } catch (Exception e) {
            throw new MessagingException("发送邮件失败: " + e.getMessage(), e);
        }
    }
}
