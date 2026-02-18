package com.stockbot.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

public class Db {
    private final String url;

    public Db(Path dbPath) throws Exception {
        Files.createDirectories(dbPath.getParent());
        this.url = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        init();
    }

    private Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(url);
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA busy_timeout=5000");
        }
        return c;
    }

    private void init() throws Exception {
        try (Connection c = connect();
             Statement st = c.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS runs (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "run_at TEXT NOT NULL," +
                    "run_mode TEXT," +
                    "label TEXT," +
                    "ai_targets INTEGER," +
                    "notes TEXT" +
                    ")");
            st.executeUpdate("CREATE TABLE IF NOT EXISTS results (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "run_id INTEGER NOT NULL," +
                    "ticker TEXT NOT NULL," +
                    "display_name TEXT," +
                    "last_close REAL," +
                    "prev_close REAL," +
                    "pct_change REAL," +
                    "fundamental REAL," +
                    "industry REAL," +
                    "macro REAL," +
                    "news REAL," +
                    "total_score REAL," +
                    "rating TEXT," +
                    "risk TEXT," +
                    "ai_ran INTEGER," +
                    "ai_summary TEXT," +
                    "gate_reason TEXT," +
                    "news_count INTEGER," +
                    "FOREIGN KEY(run_id) REFERENCES runs(id)" +
                    ")");
        }
    }

    public long insertRun(String runAt, String runMode, String label, int aiTargets, String notes) throws Exception {
        try (Connection c = connect()) {
            PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO runs(run_at, run_mode, label, ai_targets, notes) VALUES(?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, runAt);
            ps.setString(2, runMode);
            ps.setString(3, label);
            ps.setInt(4, aiTargets);
            ps.setString(5, notes);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        return -1;
    }

    public void insertResult(long runId, com.stockbot.model.StockContext sc) throws Exception {
        try (Connection c = connect()) {
            PreparedStatement ps = c.prepareStatement(
                "INSERT INTO results(run_id,ticker,display_name,last_close,prev_close,pct_change," +
                        "fundamental,industry,macro,news,total_score,rating,risk,ai_ran,ai_summary,gate_reason,news_count) " +
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps.setLong(1, runId);
            ps.setString(2, sc.ticker);
            ps.setString(3, sc.displayName);
            ps.setObject(4, sc.lastClose);
            ps.setObject(5, sc.prevClose);
            ps.setObject(6, sc.pctChange);
            ps.setObject(7, sc.factorScores.getOrDefault("fundamental", null));
            ps.setObject(8, sc.factorScores.getOrDefault("industry", null));
            ps.setObject(9, sc.factorScores.getOrDefault("macro", null));
            ps.setObject(10, sc.factorScores.getOrDefault("news", null));
            ps.setObject(11, sc.totalScore);
            ps.setString(12, sc.rating);
            ps.setString(13, sc.risk);
            ps.setInt(14, sc.aiRan ? 1 : 0);
            ps.setString(15, sc.aiSummary);
            ps.setString(16, sc.gateReason);
            ps.setInt(17, sc.news.size());
            ps.executeUpdate();
        }
    }

    public String simpleBacktestSummary(int daysLookback) throws Exception {
        // 简化回测：统计最近 N 天内高风险标记出现次数
        try (Connection c = connect()) {
            PreparedStatement ps = c.prepareStatement(
                    "SELECT COUNT(*) as n, SUM(CASE WHEN risk='RISK' THEN 1 ELSE 0 END) as r " +
                            "FROM results WHERE run_id IN (" +
                            "  SELECT id FROM runs ORDER BY id DESC LIMIT ?" +
                            ")");
            ps.setInt(1, Math.max(1, daysLookback*2)); // 粗略取值
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                long n = rs.getLong("n");
                long r = rs.getLong("r");
                return "回测（粗略）：记录数=" + n + "，高风险=" + r;
            }
        }
        return "回测（粗略）：暂无数据";
    }
}
