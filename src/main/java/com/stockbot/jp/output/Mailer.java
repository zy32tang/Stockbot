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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * SMTP mail sender.
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
        public boolean dryRun;
        public boolean failFast;
        public Path dryRunDir;
    }

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
        settings.dryRun = config.getBoolean("mail.dry_run", false);
        settings.failFast = config.getBoolean("mail.fail_fast", false);

        String customDryRunDir = config.getString("mail.dry_run.dir", "");
        settings.dryRunDir = customDryRunDir.isBlank()
                ? config.getPath("outputs.dir").resolve("mail_dry_run")
                : config.workingDir().resolve(customDryRunDir).normalize();
        return settings;
    }

    public boolean send(Settings s, String subject, String textBody, String htmlBody) throws MessagingException {
        return send(s, subject, textBody, htmlBody, List.of());
    }

    public boolean send(Settings s, String subject, String textBody, String htmlBody, List<Path> attachments) throws MessagingException {
        if (!s.enabled) {
            return false;
        }

        List<Path> safeAttachments = attachments == null ? List.of() : new ArrayList<>(attachments);
        String safeSubject = subject == null ? "" : subject;
        String safeText = textBody == null ? "" : textBody;
        String safeHtml = htmlBody == null ? "" : htmlBody;

        if (s.dryRun) {
            try {
                writeDryRunArtifacts(s, safeSubject, safeText, safeHtml, safeAttachments);
                System.out.println("Mail dry-run saved. dir=" + s.dryRunDir.toAbsolutePath());
                return true;
            } catch (Exception e) {
                handleFailure(s, "dry_run_write_failed", e);
            }
            return false;
        }

        if (isBlank(s.host) || isBlank(s.user) || isBlank(s.pass) || s.to == null || s.to.isEmpty()) {
            handleFailure(s, "smtp_settings_incomplete", new IllegalArgumentException("email enabled but smtp settings are incomplete"));
            return false;
        }

        try {
            Properties props = new Properties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host", s.host);
            props.put("mail.smtp.port", String.valueOf(s.port));

            Session session = Session.getInstance(props, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(s.user, s.pass);
                }
            });

            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(s.from));
            String toJoined = s.to.stream().map(String::trim).filter(v -> !v.isEmpty()).collect(Collectors.joining(","));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toJoined));
            message.setSubject(safeSubject, "UTF-8");

            boolean hasHtml = safeHtml != null && !safeHtml.trim().isEmpty();
            if (safeAttachments.isEmpty()) {
                if (hasHtml) {
                    message.setContent(safeHtml, "text/html; charset=UTF-8");
                } else {
                    message.setText(safeText, "UTF-8");
                }
                Transport.send(message);
                return true;
            }

            MimeMultipart mixed = new MimeMultipart("mixed");
            MimeBodyPart bodyPart = new MimeBodyPart();
            if (hasHtml) {
                bodyPart.setContent(safeHtml, "text/html; charset=UTF-8");
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
            return true;
        } catch (Exception e) {
            handleFailure(s, "smtp_send_failed", e);
            return false;
        }
    }

    private void writeDryRunArtifacts(Settings s, String subject, String textBody, String htmlBody, List<Path> attachments) throws Exception {
        Files.createDirectories(s.dryRunDir);
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault()).format(Instant.now());
        Path emlPath = s.dryRunDir.resolve("mail_" + stamp + ".eml");
        Path htmlPath = s.dryRunDir.resolve("mail_" + stamp + ".html");
        Path txtPath = s.dryRunDir.resolve("mail_" + stamp + ".txt");

        String eml = "From: " + safe(s.from) + "\n"
                + "To: " + (s.to == null ? "" : String.join(",", s.to)) + "\n"
                + "Subject: " + safe(subject) + "\n"
                + "MIME-Version: 1.0\n"
                + "Content-Type: text/html; charset=UTF-8\n\n"
                + safe(htmlBody);
        Files.writeString(emlPath, eml, StandardCharsets.UTF_8);
        Files.writeString(htmlPath, safe(htmlBody), StandardCharsets.UTF_8);
        Files.writeString(txtPath, safe(textBody), StandardCharsets.UTF_8);

        if (attachments != null && !attachments.isEmpty()) {
            Path attPath = s.dryRunDir.resolve("mail_" + stamp + "_attachments.txt");
            List<String> lines = attachments.stream()
                    .filter(p -> p != null)
                    .map(Path::toString)
                    .collect(Collectors.toList());
            Files.write(attPath, lines, StandardCharsets.UTF_8);
        }
    }

    private void handleFailure(Settings s, String stage, Exception e) throws MessagingException {
        String message = "Mail send failed stage=" + stage
                + " smtp=" + safe(s.host) + ":" + s.port
                + " from=" + maskAddress(s.from)
                + " to=" + maskAddresses(s.to)
                + " err=" + (e == null ? "" : safe(e.getMessage()));

        if (s.failFast) {
            if (e instanceof MessagingException) {
                throw (MessagingException) e;
            }
            throw new MessagingException(message, e);
        }

        System.err.println("WARN: " + message);
    }

    private String maskAddresses(List<String> to) {
        if (to == null || to.isEmpty()) {
            return "";
        }
        return to.stream().map(this::maskAddress).collect(Collectors.joining(","));
    }

    private String maskAddress(String raw) {
        String value = safe(raw);
        int at = value.indexOf('@');
        if (at <= 0) {
            return value;
        }
        String local = value.substring(0, at);
        String domain = value.substring(at + 1);
        if (local.length() <= 1) {
            return "*@" + domain;
        }
        return local.substring(0, 1) + "***@" + domain;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
