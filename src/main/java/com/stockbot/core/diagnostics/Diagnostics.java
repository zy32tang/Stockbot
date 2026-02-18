package com.stockbot.core.diagnostics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Diagnostics {
    public final long runId;
    public final String runMode;
    public final Map<String, ConfigItem> configSnapshot = new LinkedHashMap<>();
    public final Map<String, Integer> dataSourceStats = new LinkedHashMap<>();
    public final Map<String, CoverageMetric> coverages = new LinkedHashMap<>();
    public final Map<String, FeatureResolution> featureStatuses = new LinkedHashMap<>();
    public final List<GateTrace> top5Gates = new ArrayList<>();
    public final List<String> notes = new ArrayList<>();

    public String coverageScope = "MARKET";
    public String selectedFetchCoverageKey = "";
    public String selectedIndicatorCoverageKey = "";
    public String coverageSource = "";
    public String coverageOwner = "";

    public Diagnostics(long runId, String runMode) {
        this.runId = runId;
        this.runMode = runMode == null ? "" : runMode;
    }

    public void addConfig(String key, String value, String source) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        configSnapshot.put(key, new ConfigItem(key, value, source));
    }

    public void addDataSourceStat(String key, int count) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        dataSourceStats.put(key, Math.max(0, count));
    }

    public void addCoverage(String key, int numerator, int denominator, String source, String owner) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        coverages.put(key, new CoverageMetric(key, numerator, denominator, source, owner));
    }

    public void selectCoverage(
            String scope,
            String fetchCoverageKey,
            String indicatorCoverageKey,
            String source,
            String owner
    ) {
        this.coverageScope = scope == null || scope.trim().isEmpty() ? "MARKET" : scope.trim().toUpperCase();
        this.selectedFetchCoverageKey = fetchCoverageKey == null ? "" : fetchCoverageKey;
        this.selectedIndicatorCoverageKey = indicatorCoverageKey == null ? "" : indicatorCoverageKey;
        this.coverageSource = source == null ? "" : source;
        this.coverageOwner = owner == null ? "" : owner;
    }

    public void addFeatureStatus(String key, FeatureResolution status) {
        if (key == null || key.trim().isEmpty() || status == null) {
            return;
        }
        featureStatuses.put(key, status);
    }

    public void addTop5Gate(
            String gate,
            boolean passed,
            int failCount,
            String threshold,
            CauseCode causeCode,
            String owner,
            String details
    ) {
        top5Gates.add(new GateTrace(gate, passed, failCount, threshold, causeCode, owner, details));
    }

    public void addNote(String note) {
        if (note == null || note.trim().isEmpty()) {
            return;
        }
        notes.add(note.trim());
    }

    public FeatureResolution feature(String key) {
        if (key == null) {
            return null;
        }
        return featureStatuses.get(key);
    }

    public CoverageMetric selectedFetchCoverage() {
        return coverages.get(selectedFetchCoverageKey);
    }

    public CoverageMetric selectedIndicatorCoverage() {
        return coverages.get(selectedIndicatorCoverageKey);
    }

    public static final class ConfigItem {
        public final String key;
        public final String value;
        public final String source;

        private ConfigItem(String key, String value, String source) {
            this.key = key;
            this.value = value == null ? "" : value;
            this.source = source == null ? "default" : source;
        }
    }

    public static final class CoverageMetric {
        public final String key;
        public final int numerator;
        public final int denominator;
        public final double pct;
        public final String source;
        public final String owner;

        private CoverageMetric(String key, int numerator, int denominator, String source, String owner) {
            this.key = key;
            this.numerator = Math.max(0, numerator);
            this.denominator = Math.max(0, denominator);
            this.pct = this.denominator <= 0 ? 0.0 : this.numerator * 100.0 / this.denominator;
            this.source = source == null ? "" : source;
            this.owner = owner == null ? "" : owner;
        }
    }

    public static final class GateTrace {
        public final String gate;
        public final boolean passed;
        public final int failCount;
        public final String threshold;
        public final CauseCode causeCode;
        public final String owner;
        public final String details;

        private GateTrace(
                String gate,
                boolean passed,
                int failCount,
                String threshold,
                CauseCode causeCode,
                String owner,
                String details
        ) {
            this.gate = gate == null ? "" : gate;
            this.passed = passed;
            this.failCount = Math.max(0, failCount);
            this.threshold = threshold == null ? "" : threshold;
            this.causeCode = causeCode == null ? CauseCode.NONE : causeCode;
            this.owner = owner == null ? "" : owner;
            this.details = details == null ? "" : details;
        }
    }
}
