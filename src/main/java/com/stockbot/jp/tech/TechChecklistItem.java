package com.stockbot.jp.tech;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class TechChecklistItem {
    public final ChecklistStatus status;
    public final String label;
    public final String value;
    public final String rule;
}
