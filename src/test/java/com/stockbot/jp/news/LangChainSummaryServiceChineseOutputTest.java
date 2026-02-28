package com.stockbot.jp.news;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.output.Response;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LangChainSummaryServiceChineseOutputTest {

    @Test
    void summarize_shouldReturnChineseRewriteWhenFirstPassIsEnglish() {
        LangChainSummaryService service = new LangChainSummaryService(new SequencedModel(
                "<p><strong>Key Points</strong></p><ul><li>Demand improved.</li></ul>",
                "<p><strong>要点总结</strong></p><ul><li>需求持续改善且订单显著增长，利润率与现金流同步提升。</li></ul>"
        ));

        String html = service.summarize(
                "7203.T",
                "丰田汽车",
                List.of(new LangChainSummaryService.ClusterInput("订单变化", List.of("北美订单增长")))
        );

        assertTrue(html.contains("需求持续改善"));
        assertTrue(LangChainSummaryService.isMostlyChinese(html));
    }

    @Test
    void summarize_shouldFallbackToChineseWhenRewriteStillEnglish() {
        LangChainSummaryService service = new LangChainSummaryService(new SequencedModel(
                "<p><strong>Key Points</strong></p><ul><li>Demand improved.</li></ul>",
                "<p><strong>Still English</strong></p><ul><li>Nothing translated.</li></ul>"
        ));

        String html = service.summarize(
                "6758.T",
                "索尼集团",
                List.of(new LangChainSummaryService.ClusterInput("盈利", List.of("盈利预期上修")))
        );

        assertTrue(html.contains("新闻事件聚类要点"));
        assertTrue(LangChainSummaryService.isMostlyChinese(html));
    }

    @Test
    void summarize_shouldUseChineseFallbackWhenModelUnavailable() {
        LangChainSummaryService service = new LangChainSummaryService((ChatLanguageModel) null);

        String html = service.summarize(
                "9984.T",
                "软银集团",
                List.of(new LangChainSummaryService.ClusterInput("融资", List.of("子公司融资计划")))
        );

        assertTrue(html.contains("新闻事件聚类要点"));
        assertTrue(LangChainSummaryService.isMostlyChinese(html));
    }

    @Test
    void summarize_shouldReturnNoEventHtmlWhenClustersEmpty() {
        LangChainSummaryService service = new LangChainSummaryService((ChatLanguageModel) null);

        String html = service.summarize("7203.T", "丰田汽车", List.of());

        assertEquals(LangChainSummaryService.NO_EVENT_HTML, html);
    }

    @Test
    void isMostlyChinese_shouldDistinguishChineseAndEnglishText() {
        assertTrue(LangChainSummaryService.isMostlyChinese("<p>需求增长明显，风险可控。</p>"));
        assertFalse(LangChainSummaryService.isMostlyChinese("<p>Demand growth is strong with manageable risk.</p>"));
    }

    private static final class SequencedModel implements ChatLanguageModel {
        private final List<String> outputs;
        private int index = 0;

        private SequencedModel(String... outputs) {
            this.outputs = outputs == null ? List.of() : new ArrayList<>(List.of(outputs));
        }

        @Override
        public Response<AiMessage> generate(List<ChatMessage> messages) {
            if (outputs.isEmpty()) {
                return Response.from(AiMessage.aiMessage(""));
            }
            int at = Math.min(index, outputs.size() - 1);
            String text = outputs.get(at);
            index++;
            return Response.from(AiMessage.aiMessage(text));
        }
    }
}
