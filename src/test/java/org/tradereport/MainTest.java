package org.tradereport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.tradereport.Main.*;

public class MainTest {

    private static final Path OUTPUT_FILE = Paths.get("trade_report_2024.csv");

    private static BigDecimal safeDecimal(String s) {
        try {
            return (s == null || s.isBlank()) ? BigDecimal.ZERO : new BigDecimal(s);
        } catch (Exception e) {
            return BigDecimal.ZERO;
        }
    }

    private static String safeString(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    @Test
    public void testIsYear2024() {
        assertTrue(isYear2024("202401"));
        assertFalse(isYear2024("202303"));
        assertFalse(isYear2024(null));
    }

    @Test
    public void testIsGoods() {
        assertTrue(isGoods("Goods"));
        assertTrue(isGoods("goods"));
        assertFalse(isGoods("Services"));
        assertFalse(isGoods(null));
    }

    @Test
    public void testIsHs4() {
        assertTrue(isHs4("1234"));
        assertFalse(isHs4("123"));
        assertFalse(isHs4("abcd"));
        assertFalse(isHs4(null));
    }

    @Test
    public void testParseDecimalOrNull() {
        assertEquals(new BigDecimal("1234.56"), parseDecimalOrNull("1234.56"));
        assertEquals(new BigDecimal("1234.56"), parseDecimalOrNull("1,234.56"));
        assertNull(parseDecimalOrNull(""));
        assertNull(parseDecimalOrNull(null));
        assertNull(parseDecimalOrNull("invalid"));
    }

    @Test
    public void testMaxEntry() {
        Map<String, BigDecimal> map = new HashMap<>();
        map.put("A", new BigDecimal("5"));
        map.put("B", new BigDecimal("10"));
        var entry = maxEntry(map);
        assertEquals("B", entry.getKey());
        assertEquals(new BigDecimal("10"), entry.getValue());
    }

    @Test
    public void testMaxEntryEmptyMap() {
        Map<String, BigDecimal> map = new HashMap<>();
        assertNull(maxEntry(map));
    }

    @Test
    public void testFormatDecimal() {
        assertEquals("123.46", callFormatDecimal(new BigDecimal("123.456")));
        assertEquals("", callFormatDecimal(null));
    }

    private String callFormatDecimal(BigDecimal v) {
        try {
            var method = Main.class.getDeclaredMethod("formatDecimal", BigDecimal.class);
            method.setAccessible(true);
            return (String) method.invoke(null, v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBuildReport() {
        CountryStats stats = new CountryStats();
        stats.imports = new BigDecimal("100");
        stats.exports = new BigDecimal("200");
        stats.importsByProduct.put("0101", new BigDecimal("100"));
        stats.exportsByProduct.put("0202", new BigDecimal("200"));

        var report = buildReport("Testland", stats);

        assertEquals("Testland", report.label());
        assertEquals(new BigDecimal("100"), report.tradeBalance());
        assertEquals("0101", report.topImportCode());
        assertEquals("0202", report.topExportCode());
    }

    @Test
    public void testBuildReportWithEmptyStats() {
        CountryStats stats = new CountryStats();
        var report = buildReport("Emptyland", stats);

        assertEquals("Emptyland", report.label());
        assertEquals(BigDecimal.ZERO, report.tradeBalance());
        assertNull(report.topImportCode());
        assertNull(report.topExportCode());
    }

    @Test
    public void testCountryStatsAggregation() {
        CountryStats stats = new CountryStats();
        stats.imports = stats.imports.add(new BigDecimal("100"));
        stats.importsByProduct.merge("0101", new BigDecimal("100"), BigDecimal::add);
        stats.exports = stats.exports.add(new BigDecimal("50"));
        stats.exportsByProduct.merge("0202", new BigDecimal("50"), BigDecimal::add);

        assertEquals(new BigDecimal("100"), stats.imports);
        assertEquals(new BigDecimal("50"), stats.exports);
        assertEquals(new BigDecimal("100"), stats.importsByProduct.get("0101"));
        assertEquals(new BigDecimal("50"), stats.exportsByProduct.get("0202"));
    }

    @Test
    public void testGoodsClassificationLoader() throws IOException {
        Path tempFile = Files.createTempFile("goods", ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            writer.write("NZHSC_Level_2_Code_HS4,NZHSC_Level_1_Code_HS2,NZHSC_Level_2,NZHSC_Level_1,Status_HS4\n");
            writer.write("0101,01,Horses,Animals,Current\n");
            writer.write("abcd,02,Invalid,Animals,Current\n");
        }

        Map<String, String> result = GoodsClassificationLoader.loadHs4Descriptions(tempFile);
        assertEquals(1, result.size());
        assertEquals("Horses", result.get("0101"));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetThrowsWhenColumnMissing() throws IOException {
        Path tempFile = Files.createTempFile("dummy", ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            writer.write("col1,col2\n");
            writer.write("val1,val2\n");
        }

        try (BufferedReader reader = Files.newBufferedReader(tempFile, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
            CSVRecord record = parser.iterator().next();
            get(record, "nonexistent");
        }
    }

    @Test
    public void testIsImportAndIsExportViaReflection() throws Exception {
        var isImportMethod = Main.class.getDeclaredMethod("isImport", String.class);
        var isExportMethod = Main.class.getDeclaredMethod("isExport", String.class);
        isImportMethod.setAccessible(true);
        isExportMethod.setAccessible(true);

        assertTrue((boolean) isImportMethod.invoke(null, "Imports"));
        assertFalse((boolean) isImportMethod.invoke(null, "Exports"));
        assertTrue((boolean) isExportMethod.invoke(null, "Exports"));
        assertFalse((boolean) isExportMethod.invoke(null, "Imports"));
    }

    private boolean isCountry(String actual, String expected) {
        return actual != null && actual.startsWith(expected);
    }

    private List<Map<String, String>> readCsv() {
        List<Map<String, String>> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(OUTPUT_FILE, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setTrim(true)
                     .setIgnoreSurroundingSpaces(true)
                     .build()
                     .parse(reader)) {

            for (CSVRecord record : parser) {
                Map<String, String> row = new HashMap<>();
                for (String header : parser.getHeaderNames()) {
                    row.put(header, record.get(header));
                }
                rows.add(row);
            }
        } catch (IOException e) {
            fail("Could not read CSV: " + e.getMessage());
        }
        return rows;
    }

    @Test
    public void testNorwayDoesNotAffectEU() {
        List<Map<String, String>> rows = readCsv();
        BigDecimal norwayBalance = null;
        BigDecimal euBalance = null;

        for (Map<String, String> r : rows) {
            String country = safeString(r.get("Country"));
            if (isCountry(country, "Norway")) norwayBalance = safeDecimal(r.get("Trade_Balance_NZD"));
            if (isCountry(country, "European Union")) euBalance = safeDecimal(r.get("Trade_Balance_NZD"));
        }

        assertNotNull("Norway row missing", norwayBalance);
        assertNotNull("EU row missing", euBalance);

        assertTrue("EU balance incorrectly includes Norway", euBalance.compareTo(norwayBalance) != 0);
    }

    @Test
    public void testEmptyEUStillExists() {
        List<Map<String, String>> rows = readCsv();
        boolean euFound = false;
        for (Map<String, String> r : rows) {
            String country = safeString(r.get("Country"));
            if (isCountry(country, "European Union")) {
                euFound = true;
                break;
            }
        }
        assertTrue("EU aggregate row missing", euFound);
    }

    @Test
    public void testEUAggregationMatchesMembers() {
        List<Map<String, String>> rows = readCsv();

        BigDecimal euBalance = null;
        BigDecimal sumMembersBalance = BigDecimal.ZERO;

        for (Map<String, String> r : rows) {
            String country = safeString(r.get("Country"));
            BigDecimal balance = safeDecimal(r.get("Trade_Balance_NZD"));

            if (isCountry(country, "European Union")) {
                euBalance = balance;
            } else if (country != null && !country.startsWith("Norway")) {
                // include only EU members (exclude Norway)
                sumMembersBalance = sumMembersBalance.add(balance);
            }
        }

        assertNotNull("EU row missing", euBalance);

        // EU balance should equal the sum of member balances (within tolerance for rounding).
        BigDecimal diff = euBalance.subtract(sumMembersBalance).abs();
        assertTrue("EU balance mismatch too large: " + diff,
                diff.compareTo(new BigDecimal("0.01")) <= 0);
    }

}
