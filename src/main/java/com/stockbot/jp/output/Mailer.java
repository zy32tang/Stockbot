package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;
import org.simplejavamail.api.email.Email;
import org.simplejavamail.api.email.EmailPopulatingBuilder;
import org.simplejavamail.api.mailer.config.TransportStrategy;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.mailer.MailerBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * SMTP sender backed by Simple Java Mail.
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

    public boolean send(Settings settings, String subject, String textBody, String htmlBody) throws Exception {
        return send(settings, subject, textBody, htmlBody, List.of());
    }

    public boolean send(Settings settings, String subject, String textBody, String htmlBody, List<Path> attachments) throws Exception {
        if (settings == null || !settings.enabled) {
            return false;
        }
        String safeSubject = safe(subject);
        String safeText = safe(textBody);
        String safeHtml = safe(htmlBody);
        List<Path> safeAttachments = attachments == null ? List.of() : new ArrayList<>(attachments);

        if (settings.dryRun) {
            writeDryRunArtifacts(settings, safeSubject, safeText, safeHtml, safeAttachments);
            System.out.println("Mail dry-run saved. dir=" + settings.dryRunDir.toAbsolutePath());
            return true;
        }

        if (isBlank(settings.host)
                || isBlank(settings.user)
                || isBlank(settings.pass)
                || isBlank(settings.from)
                || settings.to == null
                || settings.to.isEmpty()) {
            return handleFailure(settings, "smtp_settings_incomplete", new IllegalArgumentException("missing smtp fields"));
        }

        try {
            EmailPopulatingBuilder builder = EmailBuilder.startingBlank()
                    .from(settings.from.trim())
                    .withSubject(safeSubject);
            for (String to : settings.to) {
                String recipient = safe(to).trim();
                if (!recipient.isEmpty()) {
                    builder.to(recipient);
                }
            }
            if (!safeText.isBlank()) {
                builder.withPlainText(safeText);
            }
            if (!safeHtml.isBlank()) {
                builder.withHTMLText(safeHtml);
            } else if (safeText.isBlank()) {
                builder.withPlainText("");
            }
            for (Path attachment : safeAttachments) {
                if (attachment == null || !Files.isRegularFile(attachment)) {
                    continue;
                }
                byte[] bytes = Files.readAllBytes(attachment);
                String fileName = attachment.getFileName() == null ? "attachment.bin" : attachment.getFileName().toString();
                String mimeType = Files.probeContentType(attachment);
                builder.withAttachment(fileName, bytes, mimeType == null ? "application/octet-stream" : mimeType);
            }
            Email email = builder.buildEmail();
            org.simplejavamail.api.mailer.Mailer smtpMailer = MailerBuilder
                    .withSMTPServer(settings.host.trim(), settings.port, settings.user.trim(), settings.pass)
                    .withTransportStrategy(TransportStrategy.SMTP_TLS)
                    .buildMailer();
            smtpMailer.sendMail(email);
            return true;
        } catch (Exception e) {
            return handleFailure(settings, "smtp_send_failed", e);
        }
    }

    private void writeDryRunArtifacts(Settings settings, String subject, String textBody, String htmlBody, List<Path> attachments) throws Exception {
        Files.createDirectories(settings.dryRunDir);
        String stamp = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault()).format(Instant.now());
        Path emlPath = settings.dryRunDir.resolve("mail_" + stamp + ".eml");
        Path htmlPath = settings.dryRunDir.resolve("mail_" + stamp + ".html");
        Path txtPath = settings.dryRunDir.resolve("mail_" + stamp + ".txt");
        String eml = "From: " + safe(settings.from) + "\n"
                + "To: " + (settings.to == null ? "" : String.join(",", settings.to)) + "\n"
                + "Subject: " + safe(subject) + "\n"
                + "MIME-Version: 1.0\n"
                + "Content-Type: text/html; charset=UTF-8\n\n"
                + safe(htmlBody);
        Files.writeString(emlPath, eml, StandardCharsets.UTF_8);
        Files.writeString(htmlPath, safe(htmlBody), StandardCharsets.UTF_8);
        Files.writeString(txtPath, safe(textBody), StandardCharsets.UTF_8);
        if (attachments != null && !attachments.isEmpty()) {
            Path attPath = settings.dryRunDir.resolve("mail_" + stamp + "_attachments.txt");
            List<String> lines = attachments.stream().filter(p -> p != null).map(Path::toString).collect(Collectors.toList());
            Files.write(attPath, lines, StandardCharsets.UTF_8);
        }
    }

    private boolean handleFailure(Settings settings, String stage, Exception error) throws Exception {
        String message = "Mail send failed stage=" + stage
                + " smtp=" + safe(settings.host) + ":" + settings.port
                + " from=" + maskAddress(settings.from)
                + " to=" + maskAddresses(settings.to)
                + " err=" + safe(error == null ? null : error.getMessage());
        if (settings.failFast) {
            throw new IllegalStateException(message, error);
        }
        System.err.println("WARN: " + message);
        return false;
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
