package com.stockbot.state;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class NotifyState {
    private final Map<String, String> lastSentDate = new HashMap<>();

    public static NotifyState load(Path path) {
        NotifyState s = new NotifyState();
        try {
            if (Files.exists(path)) {
                String txt = Files.readString(path, StandardCharsets.UTF_8);
                JSONObject o = new JSONObject(txt);
                for (String k : o.keySet()) s.lastSentDate.put(k, o.optString(k, ""));
            }
        } catch (Exception ignored) {}
        return s;
    }

    public boolean shouldNotify(String ticker, String risk) {
        if (!"RISK".equalsIgnoreCase(risk)) return false;
        String today = LocalDate.now().toString();
        String last = lastSentDate.get(ticker);
        return last == null || !last.equals(today);
    }

    public void markNotified(String ticker) {
        lastSentDate.put(ticker, LocalDate.now().toString());
    }

    public void save(Path path) throws IOException {
        JSONObject o = new JSONObject(lastSentDate);
        Files.createDirectories(path.getParent());
        Files.writeString(path, o.toString(2), StandardCharsets.UTF_8);
    }
}
