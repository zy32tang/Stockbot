package com.stockbot.jp.news;

import com.stockbot.jp.config.Config;
import com.stockbot.model.NewsItem;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NewsIngestorSanitizationAndTuningTest {

    @Test
    void normalizeQueries_shouldDropInvalidTokensAndUseTickerFallback() throws Exception {
        NewsIngestor ingestor = new NewsIngestor(config(Map.of()), null, null);

        List<String> normalized = invokeNormalizeQueries(
                ingestor,
                "AAPL",
                List.of(" ", "undefined", "null", "AAPL outlook", "n/a"),
                6
        );
        assertEquals(List.of("AAPL outlook"), normalized);

        List<String> fallback = invokeNormalizeQueries(
                ingestor,
                "TSLA",
                List.of("undefined", "none", ""),
                6
        );
        assertEquals(List.of("TSLA"), fallback);
    }

    @Test
    void mergeFetched_shouldFilterInvalidTitleAndSanitizeStoredFields() throws Exception {
        NewsIngestor ingestor = new NewsIngestor(config(Map.of()), null, null);
        LinkedHashMap<String, NewsItemDao.UpsertItem> merged = new LinkedHashMap<>();
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        List<NewsItem> fetched = List.of(
                new NewsItem("undefined", "ignored", "https://example.com/a", "null", now),
                new NewsItem("有效标题", "undefined", "https://www.cnbc.com/a?utm_source=x&id=7", "null", now)
        );

        invokeMergeFetched(ingestor, merged, fetched, "en", "US");

        assertEquals(1, merged.size());
        NewsItemDao.UpsertItem item = merged.values().iterator().next();
        assertEquals("有效标题", item.getTitle());
        assertEquals("有效标题", item.getContent());
        assertEquals("www.cnbc.com", item.getSource());
        assertEquals("en", item.getLang());
        assertEquals("US", item.getRegion());
        assertFalse(item.getUrl().contains("utm_"));
    }

    @Test
    void enabledSourcesSnapshot_shouldParseAllSupportedSourcesFromConfig() {
        Config config = config(Map.of(
                "watchlist.news.sources",
                "google,bing,yahoo,cnbc,marketwatch,wsj,nytimes,yahoonews,investing,ft,guardian,seekingalpha,invalid"
        ));
        NewsIngestor ingestor = new NewsIngestor(config, null, null);

        Set<String> expected = new LinkedHashSet<>(List.of(
                "google",
                "bing",
                "yahoo",
                "cnbc",
                "marketwatch",
                "wsj",
                "nytimes",
                "yahoonews",
                "investing",
                "ft",
                "guardian",
                "seekingalpha"
        ));
        assertEquals(expected, ingestor.enabledSourcesSnapshot());
    }

    @Test
    void tuningSnapshot_shouldAutoTuneOnDefaultsAndRespectOverrides() {
        NewsIngestor autoIngestor = new NewsIngestor(config(Map.of()), null, null);
        NewsIngestor.TuningSnapshot auto = autoIngestor.tuningSnapshot();

        assertEquals("accuracy", auto.getProfile());
        assertTrue(auto.getQueryVariants() >= 6);
        assertTrue(auto.getMaxResultsPerVariant() >= 8);
        assertTrue(auto.getNewsConcurrent() >= 10);
        assertTrue(auto.getVectorExpandTopK() >= 8);
        assertTrue(auto.getVectorExpandRounds() >= 2);

        NewsIngestor overrideIngestor = new NewsIngestor(config(Map.of(
                "news.performance.auto_tune", "true",
                "news.query.max_variants", "4",
                "news.query.max_results_per_variant", "5",
                "news.concurrent", "3",
                "news.vector.query_expand.top_k", "4",
                "news.vector.query_expand.rounds", "1"
        )), null, null);
        NewsIngestor.TuningSnapshot override = overrideIngestor.tuningSnapshot();

        assertEquals(4, override.getQueryVariants());
        assertEquals(5, override.getMaxResultsPerVariant());
        assertEquals(3, override.getNewsConcurrent());
        assertEquals(4, override.getVectorExpandTopK());
        assertEquals(1, override.getVectorExpandRounds());
    }

    @SuppressWarnings("unchecked")
    private static List<String> invokeNormalizeQueries(
            NewsIngestor ingestor,
            String ticker,
            List<String> queries,
            int maxVariants
    ) throws Exception {
        Method method = NewsIngestor.class.getDeclaredMethod(
                "normalizeQueries",
                String.class,
                List.class,
                int.class
        );
        method.setAccessible(true);
        return (List<String>) method.invoke(ingestor, ticker, queries, maxVariants);
    }

    @SuppressWarnings("unchecked")
    private static void invokeMergeFetched(
            NewsIngestor ingestor,
            LinkedHashMap<String, NewsItemDao.UpsertItem> merged,
            List<NewsItem> fetched,
            String lang,
            String region
    ) throws Exception {
        Method method = NewsIngestor.class.getDeclaredMethod(
                "mergeFetched",
                LinkedHashMap.class,
                List.class,
                String.class,
                String.class
        );
        method.setAccessible(true);
        method.invoke(ingestor, merged, fetched, lang, region);
    }

    private static Config config(Map<String, ?> overrides) {
        return Config.fromConfigurationProperties(Path.of("."), overrides);
    }
}

