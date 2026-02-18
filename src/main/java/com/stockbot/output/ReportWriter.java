package com.stockbot.output;

import com.stockbot.model.DailyPrice;
import com.stockbot.model.RunContext;
import com.stockbot.model.StockContext;
import com.stockbot.scoring.ScoringEngine;
import com.stockbot.utils.TextFormatter;

import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Stroke;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ReportWriter {

    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter DISPLAY_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public Path writeTxt(RunContext rc, List<StockContext> stocks, Map<String, Long> timers, String backtestSummary) throws Exception {
        Path dir = rc.outputsDir.resolve("reports");
        Files.createDirectories(dir);
        String ts = rc.startedAt.format(FILE_TS);
        Path out = dir.resolve("report_" + rc.label + "_" + ts + ".txt");

        List<StockContext> ranked = ranked(stocks);
        ScoringEngine se = new ScoringEngine();
        String newsSource = String.valueOf(rc.meta.getOrDefault("news_sources", "GoogleNewsRSS"));

        StringBuilder sb = new StringBuilder(8192);
        sb.append("【StockBot 日报】").append(rc.startedAt.format(DISPLAY_TS)).append("\n");
        sb.append("运行模式: ").append(runModeText(rc.runMode))
                .append("    标签: ").append(rc.label).append("\n");
        sb.append("AI触发: ").append(rc.meta.getOrDefault("ai_targets", 0))
                .append("    新闻来源: ").append(newsSource).append("\n");
        sb.append("策略: 仅满足门限条件的股票触发 AI 摘要（总分/新闻数/跌幅任一命中）。\n\n");

        sb.append("今日排序\n");
        for (int i = 0; i < ranked.size(); i++) {
            StockContext sc = ranked.get(i);
            sb.append(i + 1).append(". ").append(displayName(sc))
                    .append(" | 综合 ").append(fmt(sc.totalScore)).append("/10")
                    .append(" | 评级 ").append(ratingText(sc.rating))
                    .append(" | 风险 ").append(riskText(sc.risk))
                    .append(" | ").append(se.positionSuggestion(sc))
                    .append("\n");
        }
        sb.append("\n");

        sb.append("股票明细\n");
        for (StockContext sc : ranked) {
            sb.append("- ").append(displayName(sc)).append("\n");
            sb.append("  股票代码: ").append(sc.ticker).append("\n");
            sb.append("  价格: ").append(fmt(sc.lastClose))
                    .append(" (前收 ").append(fmt(sc.prevClose)).append(")")
                    .append(" 涨跌幅 ").append(fmt(sc.pctChange)).append("%\n");
            sb.append("  综合分: ").append(fmt(sc.totalScore)).append("/10")
                    .append(" | 评级 ").append(ratingText(sc.rating))
                    .append(" | 风险 ").append(riskText(sc.risk)).append("\n");
            sb.append("  因子: ").append(factorsText(sc.factorScores)).append("\n");
            sb.append("  触发原因: ").append(gateText(sc.gateReason)).append("\n");
            sb.append("  新闻数量: ").append(sc.news.size()).append("\n");
            sb.append("  ").append(se.positionSuggestion(sc)).append("\n");
            if (sc.aiRan) {
                String ai = TextFormatter.cleanForEmail(sc.aiSummary);
                sb.append("  AI解读: ").append(trimText(ai, 420)).append("\n");
            }
            sb.append("\n");
        }

        sb.append("=== 系统摘要 ===\n");
        sb.append(backtestSummary == null ? "" : backtestSummary).append("\n");
        sb.append("=== 耗时统计 ===\n");
        for (Map.Entry<String, Long> e : timers.entrySet()) {
            sb.append(timerLabel(e.getKey())).append(" = ").append(e.getValue()).append(" 毫秒\n");
        }
        sb.append("\n");
        sb.append("备注: 本报告仅作信号提醒，不构成投资建议。\n");

        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);
        return out;
    }

    public Path writePng(RunContext rc, List<StockContext> stocks) throws Exception {
        Path dir = rc.outputsDir.resolve("reports");
        Files.createDirectories(dir);
        String ts = rc.startedAt.format(FILE_TS);
        Path out = dir.resolve("report_" + rc.label + "_" + ts + ".png");

        List<StockContext> ranked = ranked(stocks);
        int width = 1300;
        int rowH = 120;
        int height = Math.max(220, 120 + ranked.size() * rowH);

        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        g.setColor(new Color(245, 248, 252));
        g.fillRect(0, 0, width, height);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 24));
        g.drawString("StockBot 总览  标签=" + rc.label + "  模式=" + runModeText(rc.runMode), 28, 48);

        int y = 92;
        g.setFont(new Font("SansSerif", Font.PLAIN, 18));
        for (int i = 0; i < ranked.size(); i++) {
            StockContext sc = ranked.get(i);
            g.setColor(Color.WHITE);
            g.fillRoundRect(20, y - 24, width - 40, rowH - 12, 14, 14);
            g.setColor(new Color(220, 228, 236));
            g.drawRoundRect(20, y - 24, width - 40, rowH - 12, 14, 14);

            g.setColor(Color.BLACK);
            g.drawString((i + 1) + ". " + displayName(sc), 36, y);
            y += 28;
            g.drawString(
                    "综合分=" + fmt(sc.totalScore)
                            + "  评级=" + ratingText(sc.rating)
                            + "  风险=" + riskText(sc.risk)
                            + "  涨跌幅=" + fmt(sc.pctChange) + "%"
                            + "  新闻=" + sc.news.size(),
                    64,
                    y
            );
            y += 28;
            g.drawString("触发=" + gateText(sc.gateReason), 64, y);
            y += 42;
        }

        g.dispose();
        ImageIO.write(img, "png", out.toFile());
        return out;
    }

    public List<Path> writeTrendCharts(RunContext rc, List<StockContext> stocks) throws Exception {
        List<Path> out = new ArrayList<>();
        Path dir = rc.outputsDir.resolve("reports").resolve("trends");
        Files.createDirectories(dir);
        String ts = rc.startedAt.format(FILE_TS);

        for (StockContext sc : ranked(stocks)) {
            if (sc.priceHistory.size() < 20) continue;
            out.add(writeTrendChart(dir, ts, sc));
        }
        return out;
    }

    public String buildEmailHtml(RunContext rc, List<StockContext> stocks, String backtestSummary, Map<String, Long> timers) {
        List<StockContext> ranked = ranked(stocks);
        ScoringEngine se = new ScoringEngine();
        String newsSource = String.valueOf(rc.meta.getOrDefault("news_sources", "GoogleNewsRSS"));

        StringBuilder sb = new StringBuilder(16384);
        sb.append("<!doctype html><html><head><meta charset='UTF-8'></head>");
        sb.append("<body style='margin:0;padding:18px;background:#f5f7fb;font-family:Arial,sans-serif;color:#1f2937;'>");
        sb.append("<div style='max-width:980px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;padding:18px 22px;'>");

        sb.append("<h2 style='margin:0 0 8px 0;'>【StockBot 日报】")
                .append(html(rc.startedAt.format(DISPLAY_TS)))
                .append("</h2>");

        sb.append("<div style='font-size:13px;color:#4b5563;line-height:1.7;'>")
                .append("运行模式: <b>").append(html(runModeText(rc.runMode))).append("</b> | ")
                .append("标签: <b>").append(html(rc.label)).append("</b> | ")
                .append("AI触发: <b>").append(rc.meta.getOrDefault("ai_targets", 0)).append("</b> | ")
                .append("新闻来源: ").append(html(newsSource))
                .append("</div>");
        sb.append("<div style='font-size:13px;color:#4b5563;line-height:1.7;margin-top:4px;'>")
                .append("策略: 仅满足门限条件的股票触发 AI 摘要（总分/新闻数/跌幅任一命中）。")
                .append("</div>");

        sb.append("<h3 style='margin:18px 0 8px 0;'>今日排序</h3>");
        sb.append("<table style='width:100%;border-collapse:collapse;font-size:13px;'>");
        sb.append("<tr style='background:#eef2ff;'>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>排名</th>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>股票</th>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>综合分</th>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>评级</th>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>风险</th>")
                .append("<th style='text-align:left;padding:8px;border:1px solid #e5e7eb;'>涨跌幅</th>")
                .append("</tr>");

        for (int i = 0; i < ranked.size(); i++) {
            StockContext sc = ranked.get(i);
            sb.append("<tr>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(i + 1).append("</td>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(html(displayName(sc))).append("</td>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(html(fmt(sc.totalScore))).append("/10</td>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(html(ratingText(sc.rating))).append("</td>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(html(riskText(sc.risk))).append("</td>")
                    .append("<td style='padding:8px;border:1px solid #e5e7eb;'>").append(html(fmt(sc.pctChange))).append("%</td>")
                    .append("</tr>");
        }
        sb.append("</table>");

        sb.append("<h3 style='margin:18px 0 8px 0;'>股票明细</h3>");
        for (int i = 0; i < ranked.size(); i++) {
            StockContext sc = ranked.get(i);
            sb.append("<div style='border:1px solid #e5e7eb;border-radius:10px;padding:12px;margin-bottom:10px;background:#fcfcff;'>");
            sb.append("<div style='font-size:16px;font-weight:700;margin-bottom:6px;'>")
                    .append(i + 1).append(". ").append(html(displayName(sc))).append("</div>");

            sb.append("<div style='font-size:13px;line-height:1.8;'>")
                    .append("股票代码: ").append(html(sc.ticker)).append("<br>")
                    .append("价格: ").append(html(fmt(sc.lastClose))).append(" (前收 ").append(html(fmt(sc.prevClose)))
                    .append(", 涨跌幅 ").append(html(fmt(sc.pctChange))).append("%)<br>")
                    .append("综合分: ").append(html(fmt(sc.totalScore))).append("/10 | 评级: ").append(html(ratingText(sc.rating)))
                    .append(" | 风险: ").append(html(riskText(sc.risk))).append("<br>")
                    .append(html(se.positionSuggestion(sc))).append("<br>")
                    .append("触发原因: ").append(html(gateText(sc.gateReason))).append("<br>")
                    .append("新闻数量: ").append(sc.news.size())
                    .append("</div>");

            if (sc.aiRan) {
                String ai = TextFormatter.cleanForEmail(sc.aiSummary);
                sb.append("<div style='margin-top:8px;padding:8px;background:#f8fafc;border-left:3px solid #2563eb;font-size:13px;line-height:1.75;'>")
                        .append("<b>AI解读:</b><br>")
                        .append(html(trimText(ai, 520)).replace("\n", "<br>"))
                        .append("</div>");
            }
            sb.append("</div>");
        }

        sb.append("<h3 style='margin:18px 0 8px 0;'>系统摘要</h3>");
        sb.append("<div style='font-size:13px;line-height:1.8;border:1px solid #e5e7eb;border-radius:8px;padding:10px;background:#f9fafb;'>");
        sb.append(html(backtestSummary == null ? "" : backtestSummary)).append("<br>");
        for (Map.Entry<String, Long> e : timers.entrySet()) {
            sb.append(html(timerLabel(e.getKey()))).append(": ").append(e.getValue()).append(" 毫秒<br>");
        }
        sb.append("</div>");

        sb.append("</div></body></html>");
        return sb.toString();
    }

    private Path writeTrendChart(Path dir, String ts, StockContext sc) throws Exception {
        List<DailyPrice> prices = sc.priceHistory;
        List<Double> ma20 = movingAverage(prices, 20);
        List<Double> ma60 = movingAverage(prices, 60);

        double latest = sc.lastClose != null ? sc.lastClose : prices.get(prices.size() - 1).close;
        double ma20Last = latestFinite(ma20, latest);
        double ma60Last = latestFinite(ma60, ma20Last);

        double buyLower = round2(ma20Last * 0.97);
        double buyUpper = round2(ma20Last * 1.01);
        double defense = round2(ma60Last * 0.95);

        int width = 1600;
        int height = 900;
        int left = 90;
        int right = 220;
        int top = 70;
        int bottom = 85;
        int pw = width - left - right;
        int ph = height - top - bottom;

        double[] range = valueRange(prices, ma20, ma60, buyLower, buyUpper, defense);
        double yMin = range[0];
        double yMax = range[1];

        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

        g.setColor(new Color(248, 250, 252));
        g.fillRect(0, 0, width, height);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 36));
        g.drawString(displayName(sc) + " 趋势 | " + actionText(sc), 72, 46);

        g.setColor(Color.BLACK);
        g.drawRect(left, top, pw, ph);

        g.setColor(new Color(220, 226, 236));
        for (int i = 0; i <= 6; i++) {
            int y = top + (int) Math.round((double) i / 6 * ph);
            g.drawLine(left, y, left + pw, y);
            double v = yMax - (yMax - yMin) * i / 6.0;
            g.setColor(new Color(86, 95, 111));
            g.setFont(new Font("SansSerif", Font.PLAIN, 20));
            g.drawString(String.format(Locale.US, "%.0f", v), 16, y + 7);
            g.setColor(new Color(220, 226, 236));
        }

        for (int i = 0; i <= 6; i++) {
            int idx = (int) Math.round((prices.size() - 1) * (i / 6.0));
            int x = toX(idx, prices.size(), left, pw);
            g.setColor(new Color(235, 240, 246));
            g.drawLine(x, top, x, top + ph);
            g.setColor(new Color(86, 95, 111));
            g.setFont(new Font("SansSerif", Font.PLAIN, 18));
            g.drawString(prices.get(idx).date.format(DateTimeFormatter.ofPattern("yyyy-MM")), x - 40, top + ph + 30);
        }

        drawSeries(g, prices, ma20, ma60, left, top, pw, ph, yMin, yMax);

        Stroke old = g.getStroke();
        g.setColor(new Color(16, 103, 196));
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 0f, new float[]{10f, 8f}, 0f));
        int yBuyUpper = toY(buyUpper, yMin, yMax, top, ph);
        int yBuyLower = toY(buyLower, yMin, yMax, top, ph);
        g.drawLine(left, yBuyUpper, left + pw, yBuyUpper);
        g.drawLine(left, yBuyLower, left + pw, yBuyLower);

        g.setColor(new Color(24, 108, 201));
        g.setStroke(new BasicStroke(2f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 0f, new float[]{12f, 6f, 2f, 6f}, 0f));
        int yDefense = toY(defense, yMin, yMax, top, ph);
        g.drawLine(left, yDefense, left + pw, yDefense);
        g.setStroke(old);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.PLAIN, 30));
        g.drawString("买点上沿 " + String.format(Locale.US, "%.0f", buyUpper), left + pw + 16, yBuyUpper + 10);
        g.drawString("买点下沿 " + String.format(Locale.US, "%.0f", buyLower), left + pw + 16, yBuyLower + 10);
        g.drawString("防守线 " + String.format(Locale.US, "%.0f", defense), left + pw + 16, yDefense + 10);

        int lx = width - 360;
        int ly = height - 210;
        g.setColor(new Color(252, 252, 252));
        g.fillRoundRect(lx, ly, 320, 170, 10, 10);
        g.setColor(new Color(180, 187, 197));
        g.drawRoundRect(lx, ly, 320, 170, 10, 10);
        drawLegendLine(g, lx + 18, ly + 34, new Color(31, 119, 180), "价格");
        drawLegendLine(g, lx + 18, ly + 68, new Color(255, 127, 14), "20日均线");
        drawLegendLine(g, lx + 18, ly + 102, new Color(44, 160, 44), "60日均线");
        drawLegendDashed(g, lx + 18, ly + 136, new Color(16, 103, 196), "买点 / 防守");

        g.dispose();

        String safeTicker = sc.ticker.replaceAll("[^A-Za-z0-9._-]", "_");
        Path out = dir.resolve("trend_" + safeTicker + "_" + ts + ".png");
        ImageIO.write(img, "png", out.toFile());
        return out;
    }

    private static void drawSeries(Graphics2D g, List<DailyPrice> prices, List<Double> ma20, List<Double> ma60,
                                   int left, int top, int pw, int ph, double yMin, double yMax) {
        Path2D pricePath = new Path2D.Double();
        Path2D ma20Path = new Path2D.Double();
        Path2D ma60Path = new Path2D.Double();

        boolean pStarted = false;
        boolean m20Started = false;
        boolean m60Started = false;

        for (int i = 0; i < prices.size(); i++) {
            int x = toX(i, prices.size(), left, pw);
            int yPrice = toY(prices.get(i).close, yMin, yMax, top, ph);
            if (!pStarted) {
                pricePath.moveTo(x, yPrice);
                pStarted = true;
            } else {
                pricePath.lineTo(x, yPrice);
            }

            double v20 = ma20.get(i);
            if (Double.isFinite(v20)) {
                int y20 = toY(v20, yMin, yMax, top, ph);
                if (!m20Started) {
                    ma20Path.moveTo(x, y20);
                    m20Started = true;
                } else {
                    ma20Path.lineTo(x, y20);
                }
            }

            double v60 = ma60.get(i);
            if (Double.isFinite(v60)) {
                int y60 = toY(v60, yMin, yMax, top, ph);
                if (!m60Started) {
                    ma60Path.moveTo(x, y60);
                    m60Started = true;
                } else {
                    ma60Path.lineTo(x, y60);
                }
            }
        }

        g.setStroke(new BasicStroke(4f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
        g.setColor(new Color(31, 119, 180));
        g.draw(pricePath);
        g.setColor(new Color(255, 127, 14));
        g.draw(ma20Path);
        g.setColor(new Color(44, 160, 44));
        g.draw(ma60Path);
    }

    private static void drawLegendLine(Graphics2D g, int x, int y, Color color, String text) {
        Stroke old = g.getStroke();
        g.setColor(color);
        g.setStroke(new BasicStroke(4f));
        g.drawLine(x, y, x + 58, y);
        g.setStroke(old);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.PLAIN, 22));
        g.drawString(text, x + 72, y + 8);
    }

    private static void drawLegendDashed(Graphics2D g, int x, int y, Color color, String text) {
        Stroke old = g.getStroke();
        g.setColor(color);
        g.setStroke(new BasicStroke(3f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 0f, new float[]{10f, 8f}, 0f));
        g.drawLine(x, y, x + 58, y);
        g.setStroke(old);

        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.PLAIN, 22));
        g.drawString(text, x + 72, y + 8);
    }

    private static List<Double> movingAverage(List<DailyPrice> prices, int window) {
        List<Double> out = new ArrayList<>(prices.size());
        double sum = 0.0;
        for (int i = 0; i < prices.size(); i++) {
            sum += prices.get(i).close;
            if (i >= window) sum -= prices.get(i - window).close;
            if (i + 1 >= window) out.add(sum / window);
            else out.add(Double.NaN);
        }
        return out;
    }

    private static double[] valueRange(List<DailyPrice> prices, List<Double> ma20, List<Double> ma60,
                                       double buyLower, double buyUpper, double defense) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        for (DailyPrice dp : prices) {
            min = Math.min(min, dp.close);
            max = Math.max(max, dp.close);
        }
        for (double v : ma20) {
            if (!Double.isFinite(v)) continue;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        for (double v : ma60) {
            if (!Double.isFinite(v)) continue;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }

        min = Math.min(min, Math.min(Math.min(buyLower, buyUpper), defense));
        max = Math.max(max, Math.max(Math.max(buyLower, buyUpper), defense));

        if (!Double.isFinite(min) || !Double.isFinite(max) || max <= min) return new double[]{0.0, 1.0};
        double pad = (max - min) * 0.08;
        return new double[]{min - pad, max + pad};
    }

    private static int toX(int index, int size, int left, int width) {
        if (size <= 1) return left;
        return left + (int) Math.round(index * 1.0 / (size - 1) * width);
    }

    private static int toY(double value, double min, double max, int top, int height) {
        if (max <= min) return top + height / 2;
        double ratio = (value - min) / (max - min);
        return top + height - (int) Math.round(ratio * height);
    }

    private static double latestFinite(List<Double> vals, double fallback) {
        for (int i = vals.size() - 1; i >= 0; i--) {
            if (Double.isFinite(vals.get(i))) return vals.get(i);
        }
        return fallback;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static String trimText(String s, int max) {
        if (s == null) return "";
        String t = s.trim();
        if (t.length() <= max) return t;
        return t.substring(0, max) + "...";
    }

    private static String fmt(Double v) {
        if (v == null) return "-";
        return String.format(Locale.US, "%.2f", v);
    }

    private static String html(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static List<StockContext> ranked(List<StockContext> stocks) {
        List<StockContext> out = new ArrayList<>(stocks);
        out.sort(Comparator.comparing((StockContext sc) -> sc.totalScore == null ? -999.0 : sc.totalScore).reversed());
        return out;
    }

    private static String displayName(StockContext sc) {
        if (sc.displayName == null || sc.displayName.trim().isEmpty()) return sc.ticker;
        return sc.displayName;
    }

    private static String actionText(StockContext sc) {
        if ("ATTACK".equalsIgnoreCase(sc.rating)) return "进攻";
        if ("DEFEND".equalsIgnoreCase(sc.rating)) return "防守";
        return "观察";
    }

    private static String ratingText(String rating) {
        if (rating == null || rating.trim().isEmpty()) return "-";
        if ("ATTACK".equalsIgnoreCase(rating)) return "进攻";
        if ("DEFEND".equalsIgnoreCase(rating)) return "防守";
        if ("NEUTRAL".equalsIgnoreCase(rating)) return "中性";
        return rating;
    }

    private static String riskText(String risk) {
        if (risk == null || risk.trim().isEmpty()) return "-";
        if ("NONE".equalsIgnoreCase(risk)) return "无";
        if ("RISK".equalsIgnoreCase(risk)) return "有";
        return risk;
    }

    private static String gateText(String gateReason) {
        if (gateReason == null || gateReason.trim().isEmpty()) return "-";
        if ("not triggered".equalsIgnoreCase(gateReason.trim())) return "未触发";
        String text = gateReason;
        text = text.replace("score<=threshold;", "总分<=阈值; ");
        text = text.replace("news>=min;", "新闻数>=最小值; ");
        text = text.replace("drop%<=threshold;", "跌幅<=阈值; ");
        return text.trim();
    }

    private static String runModeText(String runMode) {
        if (runMode == null) return "-";
        if ("manual".equalsIgnoreCase(runMode)) return "手动";
        if ("scheduled".equalsIgnoreCase(runMode)) return "定时";
        return runMode;
    }

    private static String factorsText(Map<String, Double> factorScores) {
        if (factorScores == null || factorScores.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Double> e : factorScores.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(factorNameText(e.getKey())).append("=").append(fmt(e.getValue()));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String factorNameText(String factor) {
        if (factor == null || factor.trim().isEmpty()) return "-";
        if ("fundamental".equalsIgnoreCase(factor)) return "基本面";
        if ("industry".equalsIgnoreCase(factor)) return "行业";
        if ("macro".equalsIgnoreCase(factor)) return "宏观";
        if ("news".equalsIgnoreCase(factor)) return "新闻";
        return factor;
    }

    private static String timerLabel(String step) {
        if (step == null || step.trim().isEmpty()) return "-";
        if ("TOTAL".equalsIgnoreCase(step)) return "总耗时";
        if ("DB_INIT".equalsIgnoreCase(step)) return "数据库初始化";
        if ("FETCH_ALL".equalsIgnoreCase(step)) return "全量抓取";
        if ("AI_SUMMARIZE".equalsIgnoreCase(step)) return "AI摘要";
        if ("STATE".equalsIgnoreCase(step)) return "通知状态";
        if ("DB_WRITE".equalsIgnoreCase(step)) return "数据库写入";
        if ("OUTPUT".equalsIgnoreCase(step)) return "报告输出";
        if ("EMAIL".equalsIgnoreCase(step)) return "邮件发送";
        return step;
    }
}
