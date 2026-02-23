package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class UniverseUpdateResult {
    public final boolean updated;
    public final int totalSymbols;
    public final String message;
}


