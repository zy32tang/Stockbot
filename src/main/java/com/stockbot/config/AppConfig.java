package com.stockbot.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class AppConfig {
    private final Properties props = new Properties();

    public static AppConfig load(Path workingDir) {
        AppConfig c = new AppConfig();
        // 从类路径加载默认配置
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) c.props.load(in);
        } catch (Exception ignored) {}
        // 若存在本地配置文件，则覆盖默认配置
        Path local = workingDir.resolve("config.properties");
        if (Files.exists(local)) {
            try (InputStream in = Files.newInputStream(local)) {
                c.props.load(in);
            } catch (Exception e) {
                System.err.println("WARN: failed to read local config.properties: " + e.getMessage());
            }
        }
        return c;
    }

    public String get(String k, String def) { return props.getProperty(k, def); }
    public boolean getBool(String k, boolean def) {
        String v = props.getProperty(k);
        if (v == null) return def;
        return v.trim().equalsIgnoreCase("true") || v.trim().equals("1") || v.trim().equalsIgnoreCase("yes");
    }
    public int getInt(String k, int def) {
        try { return Integer.parseInt(props.getProperty(k, String.valueOf(def)).trim()); }
        catch (Exception e) { return def; }
    }
    public double getDouble(String k, double def) {
        try { return Double.parseDouble(props.getProperty(k, String.valueOf(def)).trim()); }
        catch (Exception e) { return def; }
    }
}
