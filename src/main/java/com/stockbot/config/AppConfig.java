package com.stockbot.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 模块说明：AppConfig（class）。
 * 主要职责：承载 config 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class AppConfig {
    private final Properties props = new Properties();

/**
 * 方法说明：load，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：get，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String get(String k, String def) { return props.getProperty(k, def); }
/**
 * 方法说明：getBool，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public boolean getBool(String k, boolean def) {
        String v = props.getProperty(k);
        if (v == null) return def;
        return v.trim().equalsIgnoreCase("true") || v.trim().equals("1") || v.trim().equalsIgnoreCase("yes");
    }
/**
 * 方法说明：getInt，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int getInt(String k, int def) {
        try { return Integer.parseInt(props.getProperty(k, String.valueOf(def)).trim()); }
        catch (Exception e) { return def; }
    }
/**
 * 方法说明：getDouble，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public double getDouble(String k, double def) {
        try { return Double.parseDouble(props.getProperty(k, String.valueOf(def)).trim()); }
        catch (Exception e) { return def; }
    }
}
