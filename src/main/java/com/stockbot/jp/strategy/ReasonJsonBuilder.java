package com.stockbot.jp.strategy;

import com.stockbot.jp.model.FilterDecision;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;
import com.stockbot.jp.model.ScoreResult;
import com.stockbot.jp.tech.ChecklistStatus;
import com.stockbot.jp.tech.TechChecklistItem;
import com.stockbot.jp.tech.TechScoreResult;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 模块说明：ReasonJsonBuilder（class）。
 * 主要职责：承载 strategy 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class ReasonJsonBuilder {

/**
 * 方法说明：buildReasonsJson，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String buildReasonsJson(FilterDecision filter, RiskDecision risk, ScoreResult score) {
        JSONObject root = new JSONObject();
        root.put("filter_passed", filter.passed);
        root.put("risk_passed", risk.passed);
        root.put("score_passed", score.passed);
        root.put("filter_reasons", new JSONArray(filter.reasons));
        root.put("risk_flags", new JSONArray(risk.flags));
        root.put("risk_reasons", new JSONArray(risk.reasons));
        root.put("score_reasons", new JSONArray(score.reasons));
        root.put("filter_metrics", new JSONObject(filter.metrics));
        root.put("score_breakdown", new JSONObject(score.breakdown));
        return root.toString();
    }

    public String buildReasonsJson(TechScoreResult tech, double minScore) {
        JSONObject root = new JSONObject();
        if (tech == null) {
            root.put("filter_passed", false);
            root.put("risk_passed", false);
            root.put("score_passed", false);
            root.put("filter_reasons", new JSONArray(List.of("tech_result_missing")));
            root.put("risk_flags", new JSONArray(List.of("tech_result_missing")));
            root.put("risk_reasons", new JSONArray(List.of("tech_result_missing")));
            root.put("score_reasons", new JSONArray(List.of("tech_result_missing")));
            root.put("filter_metrics", new JSONObject());
            root.put("score_breakdown", new JSONObject());
            root.put("tech", new JSONObject());
            return root.toString();
        }

        boolean scorePassed = tech.getTrendStrength() >= minScore;
        boolean riskPassed = tech.getRiskLevel() != null && !"RISK".equalsIgnoreCase(tech.getRiskLevel().name());
        boolean filterPassed = tech.getDataStatus() != null && !"MISSING".equalsIgnoreCase(tech.getDataStatus().name());

        List<String> filterReasons = new ArrayList<>();
        List<String> riskReasons = new ArrayList<>();
        List<String> riskFlags = new ArrayList<>();
        List<String> scoreReasons = new ArrayList<>();
        JSONArray checklistArray = new JSONArray();
        if (tech.getChecklist() != null) {
            for (TechChecklistItem item : tech.getChecklist()) {
                if (item == null) {
                    continue;
                }
                JSONObject ci = new JSONObject();
                ci.put("status", item.getStatus() == null ? ChecklistStatus.WATCH.name() : item.getStatus().name());
                ci.put("label", safe(item.getLabel()));
                ci.put("value", safe(item.getValue()));
                ci.put("rule", safe(item.getRule()));
                checklistArray.put(ci);

                String compact = (safe(item.getLabel()) + ": " + safe(item.getValue())).trim();
                if (item.getStatus() == ChecklistStatus.FAIL) {
                    filterReasons.add(compact);
                    riskReasons.add(compact);
                    riskFlags.add(safe(item.getLabel()).replace(" ", "_").toLowerCase());
                } else if (item.getStatus() == ChecklistStatus.WATCH) {
                    filterReasons.add(compact);
                }
            }
        }
        scoreReasons.add("trend_strength=" + tech.getTrendStrength());
        if (!scorePassed) {
            scoreReasons.add("below_min_score");
        }

        JSONObject filterMetrics = new JSONObject();
        filterMetrics.put("trend_strength", tech.getTrendStrength());
        filterMetrics.put("data_status", tech.getDataStatus() == null ? "" : tech.getDataStatus().name());
        filterMetrics.put("signal_status", tech.getSignalStatus() == null ? "" : tech.getSignalStatus().name());
        filterMetrics.put("risk_level", tech.getRiskLevel() == null ? "" : tech.getRiskLevel().name());
        filterMetrics.put("bias", round4(tech.getBias()));
        filterMetrics.put("vol_ratio", round4(tech.getVolRatio()));
        filterMetrics.put("stop_pct", round4(tech.getStopPct()));

        JSONObject scoreBreakdown = new JSONObject();
        scoreBreakdown.put("trend_structure", tech.getSubscores() == null ? 0 : tech.getSubscores().getTrendStructure());
        scoreBreakdown.put("bias_risk", tech.getSubscores() == null ? 0 : tech.getSubscores().getBiasRisk());
        scoreBreakdown.put("volume_confirm", tech.getSubscores() == null ? 0 : tech.getSubscores().getVolumeConfirm());
        scoreBreakdown.put("execution_quality", tech.getSubscores() == null ? 0 : tech.getSubscores().getExecutionQuality());
        scoreBreakdown.put("final", tech.getTrendStrength());

        root.put("filter_passed", filterPassed);
        root.put("risk_passed", riskPassed);
        root.put("score_passed", scorePassed);
        root.put("filter_reasons", new JSONArray(filterReasons));
        root.put("risk_flags", new JSONArray(riskFlags));
        root.put("risk_reasons", new JSONArray(riskReasons));
        root.put("score_reasons", new JSONArray(scoreReasons));
        root.put("filter_metrics", filterMetrics);
        root.put("score_breakdown", scoreBreakdown);
        root.put("tech", buildTechObject(tech, checklistArray));
        return root.toString();
    }

/**
 * 方法说明：buildIndicatorsJson，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String buildIndicatorsJson(IndicatorSnapshot ind) {
        JSONObject root = new JSONObject();
        root.put("last_close", round4(ind.lastClose));
        root.put("sma20", round4(ind.sma20));
        root.put("sma60", round4(ind.sma60));
        root.put("sma60_prev5", round4(ind.sma60Prev5));
        root.put("sma60_slope", round4(ind.sma60Slope));
        root.put("sma120", round4(ind.sma120));
        root.put("rsi14", round4(ind.rsi14));
        root.put("atr14", round4(ind.atr14));
        root.put("atr_pct", round4(ind.atrPct));
        root.put("bollinger_upper", round4(ind.bollingerUpper));
        root.put("bollinger_middle", round4(ind.bollingerMiddle));
        root.put("bollinger_lower", round4(ind.bollingerLower));
        root.put("drawdown120_pct", round4(ind.drawdown120Pct));
        root.put("volatility20_pct", round4(ind.volatility20Pct));
        root.put("volatility20_ratio", round4(ind.volatility20Pct / 100.0));
        root.put("avg_volume20", round4(ind.avgVolume20));
        root.put("volume_ratio20", round4(ind.volumeRatio20));
        root.put("pct_from_sma20", round4(ind.pctFromSma20));
        root.put("pct_from_sma60", round4(ind.pctFromSma60));
        root.put("return3d_pct", round4(ind.return3dPct));
        root.put("return5d_pct", round4(ind.return5dPct));
        root.put("return10d_pct", round4(ind.return10dPct));
        root.put("low_lookback", round4(ind.lowLookback));
        root.put("high_lookback", round4(ind.highLookback));
        return root.toString();
    }

    public String buildIndicatorsJson(TechScoreResult tech) {
        if (tech == null) {
            return "{}";
        }
        JSONObject root = new JSONObject();
        root.put("last_close", round4(tech.getPrice()));
        root.put("ma5", round4(tech.getMa5()));
        root.put("ma10", round4(tech.getMa10()));
        root.put("ma20", round4(tech.getMa20()));
        root.put("bias", round4(tech.getBias()));
        root.put("vol_ratio", round4(tech.getVolRatio()));
        root.put("stop_line", round4(tech.getStopLine()));
        root.put("stop_pct", round4(tech.getStopPct()));
        root.put("trend_strength", tech.getTrendStrength());
        root.put("signal_status", tech.getSignalStatus() == null ? "" : tech.getSignalStatus().name());
        root.put("risk_level", tech.getRiskLevel() == null ? "" : tech.getRiskLevel().name());
        root.put("data_status", tech.getDataStatus() == null ? "" : tech.getDataStatus().name());
        if (tech.getSubscores() != null) {
            root.put("subscores", new JSONObject()
                    .put("trend_structure", tech.getSubscores().getTrendStructure())
                    .put("bias_risk", tech.getSubscores().getBiasRisk())
                    .put("volume_confirm", tech.getSubscores().getVolumeConfirm())
                    .put("execution_quality", tech.getSubscores().getExecutionQuality()));
        } else {
            root.put("subscores", new JSONObject());
        }
        return root.toString();
    }

    private JSONObject buildTechObject(TechScoreResult tech, JSONArray checklistArray) {
        JSONObject techObject = new JSONObject();
        techObject.put("trend_strength", tech.getTrendStrength());
        techObject.put("signal_status", tech.getSignalStatus() == null ? "" : tech.getSignalStatus().name());
        techObject.put("risk_level", tech.getRiskLevel() == null ? "" : tech.getRiskLevel().name());
        techObject.put("data_status", tech.getDataStatus() == null ? "" : tech.getDataStatus().name());
        techObject.put("ma", new JSONObject()
                .put("ma5", round4(tech.getMa5()))
                .put("ma10", round4(tech.getMa10()))
                .put("ma20", round4(tech.getMa20())));
        techObject.put("bias", round4(tech.getBias()));
        techObject.put("vol_ratio", round4(tech.getVolRatio()));
        techObject.put("stop_line", round4(tech.getStopLine()));
        techObject.put("stop_pct", round4(tech.getStopPct()));
        if (tech.getSubscores() != null) {
            techObject.put("subscores", new JSONObject()
                    .put("trend_structure", tech.getSubscores().getTrendStructure())
                    .put("bias_risk", tech.getSubscores().getBiasRisk())
                    .put("volume_confirm", tech.getSubscores().getVolumeConfirm())
                    .put("execution_quality", tech.getSubscores().getExecutionQuality()));
        } else {
            techObject.put("subscores", new JSONObject());
        }
        techObject.put("checklist", checklistArray == null ? new JSONArray() : checklistArray);
        return techObject;
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

/**
 * 方法说明：round4，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}
