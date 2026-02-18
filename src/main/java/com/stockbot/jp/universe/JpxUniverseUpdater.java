package com.stockbot.jp.universe;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.MetadataDao;
import com.stockbot.jp.db.UniverseDao;
import com.stockbot.jp.model.UniverseRecord;
import com.stockbot.jp.model.UniverseUpdateResult;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class JpxUniverseUpdater {
    private static final String META_LAST_SYNC = "jpx.universe.last_sync_at";
    private static final String SOURCE = "JPX";

    private final Config config;
    private final MetadataDao metadataDao;
    private final UniverseDao universeDao;
    private final HttpClient httpClient;

    public JpxUniverseUpdater(Config config, MetadataDao metadataDao, UniverseDao universeDao) {
        this.config = config;
        this.metadataDao = metadataDao;
        this.universeDao = universeDao;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public UniverseUpdateResult updateIfNeeded(boolean forceUpdate) throws Exception {
        boolean force = forceUpdate || config.getBoolean("jpx.universe.force_update");
        int existing = universeDao.countActive();
        int refreshDays = Math.max(1, config.getInt("jpx.universe.refresh_days", 7));

        if (!force && existing > 0) {
            Optional<String> lastSync = metadataDao.get(META_LAST_SYNC);
            if (lastSync.isPresent()) {
                Instant last = parseInstant(lastSync.get());
                if (last != null) {
                    long days = ChronoUnit.DAYS.between(last, Instant.now());
                    if (days < refreshDays) {
                        return new UniverseUpdateResult(false, existing, "universe is fresh (" + days + " days old)");
                    }
                }
            }
        }

        String url = config.requireString("jpx.universe.url");
        byte[] bytes = download(url);
        List<UniverseRecord> records = parseUniverse(url, bytes);
        if (records.isEmpty()) {
            throw new IllegalStateException("JPX parse returned 0 records. URL=" + url);
        }

        int upserted = universeDao.replaceFromSource(SOURCE, records);
        metadataDao.put(META_LAST_SYNC, Instant.now().toString());
        return new UniverseUpdateResult(true, upserted, "updated from JPX");
    }

    private byte[] download(String url) throws Exception {
        int timeoutSec = Math.max(10, config.getInt("stooq.request_timeout_sec", 20));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSec))
                .header("User-Agent", "stockbot-jp/1.0")
                .GET()
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("JPX download failed: HTTP " + response.statusCode());
        }
        return response.body();
    }

    private List<UniverseRecord> parseUniverse(String url, byte[] bytes) throws Exception {
        String lower = url.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".csv")) {
            return parseCsv(bytes);
        }
        return parseExcel(bytes);
    }

    private List<UniverseRecord> parseExcel(byte[] bytes) throws Exception {
        try (Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(bytes))) {
            DataFormatter formatter = new DataFormatter(Locale.JAPAN);
            List<UniverseRecord> best = new ArrayList<>();
            for (int s = 0; s < wb.getNumberOfSheets(); s++) {
                Sheet sheet = wb.getSheetAt(s);
                List<UniverseRecord> parsed = parseExcelByHeader(sheet, formatter);
                if (parsed.size() > best.size()) {
                    best = parsed;
                }
                if (best.size() >= 1000) {
                    break;
                }
            }
            if (best.isEmpty()) {
                for (int s = 0; s < wb.getNumberOfSheets(); s++) {
                    Sheet sheet = wb.getSheetAt(s);
                    List<UniverseRecord> parsed = parseExcelByHeuristic(sheet, formatter);
                    if (parsed.size() > best.size()) {
                        best = parsed;
                    }
                }
            }
            return best;
        }
    }

    private List<UniverseRecord> parseExcelByHeader(Sheet sheet, DataFormatter formatter) {
        List<UniverseRecord> out = new ArrayList<>();
        Set<String> dedup = new HashSet<>();
        HeaderMapping mapping = detectHeader(sheet, formatter);
        if (mapping.codeCol < 0 || mapping.nameCol < 0) {
            return out;
        }

        for (int r = mapping.headerRow + 1; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            String code = normalizeCode(getCellText(row, mapping.codeCol, formatter));
            if (code.isEmpty()) {
                continue;
            }
            String name = getCellText(row, mapping.nameCol, formatter).trim();
            if (name.isEmpty()) {
                continue;
            }
            String market = mapping.marketCol < 0 ? "" : getCellText(row, mapping.marketCol, formatter).trim();
            String ticker = code + ".jp";
            if (dedup.add(ticker)) {
                out.add(new UniverseRecord(ticker, code, name, market));
            }
        }
        return out;
    }

    private List<UniverseRecord> parseExcelByHeuristic(Sheet sheet, DataFormatter formatter) {
        List<UniverseRecord> out = new ArrayList<>();
        Set<String> dedup = new HashSet<>();
        for (int r = 0; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            String code = "";
            String name = "";
            String market = "";
            int codeCol = -1;
            int limit = Math.min(16, Math.max(0, row.getLastCellNum()));
            for (int c = 0; c < limit; c++) {
                String text = getCellText(row, c, formatter).trim();
                if (text.isEmpty()) {
                    continue;
                }
                if (code.isEmpty()) {
                    String normalizedCode = normalizeCode(text);
                    if (!normalizedCode.isEmpty()) {
                        code = normalizedCode;
                        codeCol = c;
                        continue;
                    }
                } else if (name.isEmpty() && c > codeCol) {
                    if (looksLikeName(text)) {
                        name = text;
                        continue;
                    }
                } else if (!name.isEmpty() && market.isEmpty() && c > codeCol) {
                    market = text;
                    break;
                }
            }
            if (!code.isEmpty() && !name.isEmpty()) {
                String ticker = code + ".jp";
                if (dedup.add(ticker)) {
                    out.add(new UniverseRecord(ticker, code, name, market));
                }
            }
        }
        return out;
    }

    private List<UniverseRecord> parseCsv(byte[] bytes) {
        String text = decodeCsv(bytes);
        String[] lines = text.split("\\r?\\n");
        int codeCol = -1;
        int nameCol = -1;
        int marketCol = -1;

        List<UniverseRecord> out = new ArrayList<>();
        Set<String> dedup = new HashSet<>();

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) {
                continue;
            }
            List<String> cols = splitCsvLine(line);
            if (i == 0) {
                for (int c = 0; c < cols.size(); c++) {
                    String normalized = normalizeHeader(cols.get(c));
                    if (isCodeHeader(normalized)) {
                        codeCol = c;
                    } else if (isNameHeader(normalized)) {
                        nameCol = c;
                    } else if (isMarketHeader(normalized)) {
                        marketCol = c;
                    }
                }
                continue;
            }
            if (codeCol < 0 || nameCol < 0 || cols.size() <= Math.max(codeCol, nameCol)) {
                continue;
            }

            String code = normalizeCode(cols.get(codeCol));
            if (code.isEmpty()) {
                continue;
            }
            String name = cols.get(nameCol).trim();
            if (name.isEmpty()) {
                continue;
            }
            String market = marketCol >= 0 && cols.size() > marketCol ? cols.get(marketCol).trim() : "";
            String ticker = code + ".jp";
            if (dedup.add(ticker)) {
                out.add(new UniverseRecord(ticker, code, name, market));
            }
        }

        return out;
    }

    private HeaderMapping detectHeader(Sheet sheet, DataFormatter formatter) {
        int bestRow = -1;
        int bestCode = -1;
        int bestName = -1;
        int bestMarket = -1;

        for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 40); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            int codeCol = -1;
            int nameCol = -1;
            int marketCol = -1;
            for (Cell cell : row) {
                String normalized = normalizeHeader(formatter.formatCellValue(cell));
                if (codeCol < 0 && isCodeHeader(normalized)) {
                    codeCol = cell.getColumnIndex();
                } else if (nameCol < 0 && isNameHeader(normalized)) {
                    nameCol = cell.getColumnIndex();
                } else if (marketCol < 0 && isMarketHeader(normalized)) {
                    marketCol = cell.getColumnIndex();
                }
            }
            if (codeCol >= 0 && nameCol >= 0) {
                bestRow = r;
                bestCode = codeCol;
                bestName = nameCol;
                bestMarket = marketCol;
                break;
            }
        }
        return new HeaderMapping(bestRow, bestCode, bestName, bestMarket);
    }

    private String getCellText(Row row, int colIndex, DataFormatter formatter) {
        if (row == null || colIndex < 0) {
            return "";
        }
        Cell cell = row.getCell(colIndex);
        if (cell == null) {
            return "";
        }
        return formatter.formatCellValue(cell);
    }

    private String normalizeCode(String input) {
        if (input == null) {
            return "";
        }
        String raw = input.trim().toUpperCase(Locale.ROOT).replace("　", "");
        if (raw.matches("[0-9A-Z]{4}")) {
            return raw;
        }
        String digits = raw.replaceAll("[^0-9]", "");
        if (digits.length() > 4) {
            return digits.substring(0, 4);
        }
        return digits.length() == 4 ? digits : "";
    }

    private String normalizeHeader(String text) {
        if (text == null) {
            return "";
        }
        return text.toLowerCase(Locale.ROOT)
                .replace(" ", "")
                .replace("　", "")
                .replace("-", "")
                .replace("_", "");
    }

    private boolean isCodeHeader(String normalized) {
        return normalized.contains("code")
                || normalized.contains("銘柄コード")
                || normalized.contains("コード");
    }

    private boolean isNameHeader(String normalized) {
        return normalized.contains("銘柄名")
                || normalized.contains("name")
                || normalized.equals("銘柄");
    }

    private boolean isMarketHeader(String normalized) {
        return normalized.contains("市場")
                || normalized.contains("商品区分")
                || normalized.contains("market");
    }

    private boolean looksLikeName(String text) {
        String value = text.trim();
        if (value.isEmpty()) {
            return false;
        }
        if (normalizeCode(value).length() == 4) {
            return false;
        }
        String normalized = normalizeHeader(value);
        if (isCodeHeader(normalized) || isNameHeader(normalized) || isMarketHeader(normalized)) {
            return false;
        }
        return true;
    }

    private String decodeCsv(byte[] bytes) {
        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        int replacementCount = utf8.length() - utf8.replace("\uFFFD", "").length();
        if (replacementCount > 10) {
            Charset ms932 = Charset.forName("MS932");
            return new String(bytes, ms932);
        }
        return utf8;
    }

    private List<String> splitCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (inQuote && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuote = !inQuote;
                }
            } else if (ch == ',' && !inQuote) {
                out.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        out.add(current.toString());
        return out;
    }

    private Instant parseInstant(String text) {
        try {
            return Instant.parse(text);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static final class HeaderMapping {
        final int headerRow;
        final int codeCol;
        final int nameCol;
        final int marketCol;

        private HeaderMapping(int headerRow, int codeCol, int nameCol, int marketCol) {
            this.headerRow = headerRow;
            this.codeCol = codeCol;
            this.nameCol = nameCol;
            this.marketCol = marketCol;
        }
    }
}
