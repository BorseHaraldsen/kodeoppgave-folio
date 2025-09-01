package org.tradereport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

// I chose CSV parser library to not reinvent the wheel and write in Java, avoid issues with things such as quotation marks, commas etc... Also lots of documentation :).
// Performs quite alright on large files, however straight BufferedReader or Pandas could maybe be faster in clean data or smaller datasets.

public class Main {

    // Country code for Norway - used to filter data to only Norwegian trade.
    private static final String NORWAY_CODE = "NO";

    // EU-27 member countries (2024) - ISO2 codes. Just writing codes to avoid parsing country_classification.csv as well.
    private static final Set<String> EU_COUNTRIES = Set.of(
            "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE",
            "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV",
            "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
            "SI", "ES", "SE"
    );

    // Putting country names here associated with codes for printing.
    private static final Map<String, String> COUNTRY_NAMES = Map.ofEntries(
            Map.entry("AT", "Austria"),
            Map.entry("BE", "Belgium"),
            Map.entry("BG", "Bulgaria"),
            Map.entry("HR", "Croatia"),
            Map.entry("CY", "Cyprus"),
            Map.entry("CZ", "Czechia"),
            Map.entry("DK", "Denmark"),
            Map.entry("EE", "Estonia"),
            Map.entry("FI", "Finland"),
            Map.entry("FR", "France"),
            Map.entry("DE", "Germany"),
            Map.entry("GR", "Greece"),
            Map.entry("HU", "Hungary"),
            Map.entry("IE", "Ireland"),
            Map.entry("IT", "Italy"),
            Map.entry("LV", "Latvia"),
            Map.entry("LT", "Lithuania"),
            Map.entry("LU", "Luxembourg"),
            Map.entry("MT", "Malta"),
            Map.entry("NL", "Netherlands"),
            Map.entry("PL", "Poland"),
            Map.entry("PT", "Portugal"),
            Map.entry("RO", "Romania"),
            Map.entry("SK", "Slovakia"),
            Map.entry("SI", "Slovenia"),
            Map.entry("ES", "Spain"),
            Map.entry("SE", "Sweden"),
            Map.entry("NO", "Norway")
    );

    // Year prefix for filtering to 2024 data only (format: YYYYMM in the dataset).
    private static final String YEAR_PREFIX = "2024";

    // Default file paths.
    // By default, the program expects the CSV files in the same folder as the program.
    // (output_csv_full.csv, goods_classification.csv).
    // May also override using JVM system properties (-Dtrade.data.path=...).
    // or environment variables (TRADE_DATA_PATH, GOODS_CLASS_PATH, TRADE_RESULTS_PATH).
    private static final String DEFAULT_FILE_PATH = System.getProperty("trade.data.path",
            System.getenv().getOrDefault("TRADE_DATA_PATH", "output_csv_full.csv"));

    private static final String DEFAULT_CLASS_PATH = System.getProperty("goods.classification.path",
            System.getenv().getOrDefault("GOODS_CLASS_PATH", "goods_classification.csv"));

    private static final String DEFAULT_RESULTS_PATH = System.getProperty("trade.results.path",
            System.getenv().getOrDefault("TRADE_RESULTS_PATH", "trade_report_2024.csv"));


    // Simple container for country statistics
    // Kind of used as a DTO. Way to store stats related to a country.
    // Using BigDecimal for accurate summation of money values in case of some data with many decimals.
    // However, double most likely fine too in this particular dataset.
    static class CountryStats {
        BigDecimal imports = BigDecimal.ZERO;
        BigDecimal exports = BigDecimal.ZERO;
        Map<String, BigDecimal> importsByProduct = new HashMap<>();
        Map<String, BigDecimal> exportsByProduct = new HashMap<>();
    }

    // DTO for final computed results of one country.
    // Separates math logic from printing.
    record CountryReport(
            String label,
            BigDecimal tradeBalance,
            String topImportCode,
            BigDecimal topImportValue,
            String topExportCode,
            BigDecimal topExportValue
    ) {}

    public static void main(String[] args) {

        // Use command line argument if provided, otherwise fall back to configurable default.
        String filePath = args.length > 0 ? args[0] : DEFAULT_FILE_PATH;

        // Check if input file exists.
        Path csvPath = Paths.get(filePath);
        if (!Files.exists(csvPath)) {
            System.err.println("File not found: " + csvPath.toAbsolutePath());
            return;
        }

        // Load HS4 descriptions/products into a map for print.
        Map<String, String> hs4Descriptions;
        try {
            hs4Descriptions = GoodsClassificationLoader.loadHs4Descriptions(Paths.get(DEFAULT_CLASS_PATH));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load goods_classification.csv", e);
        }

        // Store all country stats in one map. Key is String, country code. Value is CountryStats object, which is import / export.
        // One entry for Norway, one for each EU country, and one for combined EU stats.
        Map<String, CountryStats> allStats = new HashMap<>();
        allStats.put(NORWAY_CODE, new CountryStats());
        for (String eu : EU_COUNTRIES) {
            allStats.put(eu, new CountryStats());
        }
        allStats.put("EU", new CountryStats()); // Combined EU stats (aggregate of the 27)

        //  Open the file and create a streaming CSV parser.
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setTrim(true)
                     .setIgnoreSurroundingSpaces(true)
                     .build()
                     .parse(reader)) {

            //  Iterate rows once, apply filters, and update country/product totals.
            for (CSVRecord r : parser) {
                String timeRef     = get(r, "time_ref");
                String account     = get(r, "account");
                String code        = get(r, "code");
                String countryCode = get(r, "country_code");
                String productType = get(r, "product_type");
                String valueStr    = get(r, "value");

                // Skip rows that are not 2024, not goods, not HS4 code.
                if (!isYear2024(timeRef)) continue;
                if (!isGoods(productType)) continue;
                if (!isHs4(code)) continue;

                // Parse the value, and skip if missing or invalid. Remember 0 is a valid value.
                BigDecimal v = parseDecimalOrNull(valueStr);
                if (v == null) continue;

                // Get the stats object for the country. Skip if the country is not in the list.
                CountryStats stats = allStats.get(countryCode);
                if (stats == null) continue;

                // Update import/export totals for the country and product.
                // Add the value to the country's total imports or exports.
                // Also add to the product-specific totals.
                if (isImport(account)) {
                    stats.imports = stats.imports.add(v);
                    stats.importsByProduct.merge(code, v, BigDecimal::add);
                } else if (isExport(account)) {
                    stats.exports = stats.exports.add(v);
                    stats.exportsByProduct.merge(code, v, BigDecimal::add);
                }

                // If it's an EU country, also add to EU aggregate ("EU").
                // Same as above adding value to imports/exports and product-specific totals.
                if (EU_COUNTRIES.contains(countryCode)) {
                    CountryStats eu = allStats.get("EU");
                    if (isImport(account)) {
                        eu.imports = eu.imports.add(v);
                        eu.importsByProduct.merge(code, v, BigDecimal::add);
                    } else if (isExport(account)) {
                        eu.exports = eu.exports.add(v);
                        eu.exportsByProduct.merge(code, v, BigDecimal::add);
                    }
                }
            }

            // Print results to console.
            printConsoleResults(allStats, hs4Descriptions);

            // Write results to CSV.
            writeCsvResults(Paths.get(DEFAULT_RESULTS_PATH), allStats, hs4Descriptions);

            // Print out where results are saved.
            System.out.println("\nResults saved to: " + Paths.get(DEFAULT_RESULTS_PATH).toAbsolutePath());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Convert CountryStats into a CountryReport with all math done.
    static CountryReport buildReport(String label, CountryStats stats) {
        // Standard definition: Trade balance = Exports - Imports
        // Task wording says "differansen mellom import og eksport", which sounds like imports - exports.
        // I took an active decision and went with standard trade balance definition.
        BigDecimal balance = stats.exports.subtract(stats.imports);

        // Find the highest-valued imported product (code -> total value).
        Map.Entry<String, BigDecimal> topImp = maxEntry(stats.importsByProduct);
        // Find the highest-valued exported product.
        Map.Entry<String, BigDecimal> topExp = maxEntry(stats.exportsByProduct);

        // Package all derived values into an immutable DTO for easy, side-effect-free consumption.
        return new CountryReport(
                label,
                balance,
                topImp != null ? topImp.getKey() : null,
                topImp != null ? topImp.getValue() : null,
                topExp != null ? topExp.getKey() : null,
                topExp != null ? topExp.getValue() : null
        );
    }

    // Print formatted results to console.
    private static void printConsoleResults(Map<String, CountryStats> allStats, Map<String, String> hs4Descriptions) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TRADE REPORT 2024 - NORWAY & EU (Goods, HS4)");
        System.out.println("=".repeat(80));

        printCountryReport(buildReport("Norway (NO)", allStats.get("NO")), hs4Descriptions);

        System.out.println("\n--- EU MEMBER COUNTRIES ---");
        EU_COUNTRIES.stream()
                .map(code -> COUNTRY_NAMES.get(code) + " (" + code + ")")
                .sorted() // Sort alphabetically by country name
                .forEach(label -> {
                    String code = label.substring(label.indexOf("(") + 1, label.indexOf(")"));
                    printCountryReport(buildReport(label, allStats.get(code)), hs4Descriptions);
                });

        System.out.println("\n--- EU TOTAL (27 countries) ---");
        printCountryReport(buildReport("European Union (EU)", allStats.get("EU")), hs4Descriptions);
    }

    // Print one country's results.
    private static void printCountryReport(CountryReport report, Map<String, String> hs4Descriptions) {
        System.out.printf("\n%s:\n", report.label());
        System.out.printf("  Trade balance (Exports - Imports): %,.2f NZD\n", report.tradeBalance());

        if (report.topImportCode() != null) {
            String desc = hs4Descriptions.getOrDefault(report.topImportCode(), "(unknown)");
            System.out.printf("  Most imported product: %s (%s) - %,.2f NZD\n",
                    desc, report.topImportCode(), report.topImportValue());
        } else {
            System.out.println("  Most imported product: n/a");
        }

        if (report.topExportCode() != null) {
            String desc = hs4Descriptions.getOrDefault(report.topExportCode(), "(unknown)");
            System.out.printf("  Most exported product: %s (%s) - %,.2f NZD\n",
                    desc, report.topExportCode(), report.topExportValue());
        } else {
            System.out.println("  Most exported product: n/a");
        }
    }

    // Write results to CSV file.
    private static void writeCsvResults(Path outputPath,
                                        Map<String, CountryStats> allStats,
                                        Map<String, String> hs4Descriptions) throws IOException {

        // Ensure parent directories exist.
        Path parent = outputPath.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);

        // CSV writing.
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8);
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder()
                     .setHeader("Country",
                             "Trade_Balance_NZD",
                             "Top_Import_Description", "Top_Import_Code", "Top_Import_Value_NZD",
                             "Top_Export_Description", "Top_Export_Code", "Top_Export_Value_NZD")
                     .build())) {

            // Norway.
            writeCountryRow(printer, buildReport("Norway (NO)", allStats.get("NO")), hs4Descriptions);

            // EU countries sorted alphabetically by name.
            EU_COUNTRIES.stream()
                    .sorted(Comparator.comparing(code -> COUNTRY_NAMES.get(code)))
                    .forEach(code -> {
                        try {
                            writeCountryRow(printer, buildReport(COUNTRY_NAMES.get(code) + " (" + code + ")", allStats.get(code)), hs4Descriptions);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

            // EU Total.
            writeCountryRow(printer, buildReport("European Union (EU)", allStats.get("EU")), hs4Descriptions);
        }
    }

    // Write one country's results into CSV.
    private static void writeCountryRow(CSVPrinter printer, CountryReport report,
                                        Map<String, String> hs4Descriptions) throws IOException {

        printer.printRecord(
                report.label(),
                formatDecimal(report.tradeBalance()),

                // Import description and fields (empty strings if no top import)
                report.topImportCode() != null ? hs4Descriptions.getOrDefault(report.topImportCode(), "") : "",
                report.topImportCode() != null ? report.topImportCode() : "",
                report.topImportValue() != null ? formatDecimal(report.topImportValue()) : "",

                // Export description and fields (empty strings if no top export)
                report.topExportCode() != null ? hs4Descriptions.getOrDefault(report.topExportCode(), "") : "",
                report.topExportCode() != null ? report.topExportCode() : "",
                report.topExportValue() != null ? formatDecimal(report.topExportValue()) : ""
        );
    }

    //Helpers.

    // Get column value by name, throw if missing.
    static String get(CSVRecord r, String name) {
        try { return r.get(name); }
        catch (IllegalArgumentException e) {
            throw new IllegalStateException("Missing expected column: " + name, e);
        }
    }

    // Filter helper, for 2024 year only.
    static boolean isYear2024(String timeRef) {
        return timeRef != null && timeRef.startsWith(YEAR_PREFIX);
    }

    // Filter helper, for product type "Goods" only.
    static boolean isGoods(String productType) {
        return productType != null && productType.trim().equalsIgnoreCase("Goods");
    }

    // Filter helper, for HS4 codes only (4 digits).
    static boolean isHs4(String code) {
        return code != null && code.matches("\\d{4}");
    }

    // Filter helper, for import account only.
    private static boolean isImport(String account) {
        return account != null && account.trim().equalsIgnoreCase("Imports");
    }

    // Filter helper, for export account only.
    private static boolean isExport(String account) {
        return account != null && account.trim().equalsIgnoreCase("Exports");
    }

    // Parse decimal value without commas for parsing correctly, return null if invalid or missing.
    static BigDecimal parseDecimalOrNull(String s) {
        if (s == null || s.isBlank()) return null;
        try { return new BigDecimal(s.replace(",", "")); }
        catch (NumberFormatException e) { return null; }
    }

    // Find the map entry with the maximum value, or null if map is empty.
    static Map.Entry<String, BigDecimal> maxEntry(Map<String, BigDecimal> map) {
        if (map.isEmpty()) return null;
        return map.entrySet().stream().max(Map.Entry.comparingByValue()).orElse(null);
    }

    // Format BigDecimal to string with 2 decimal places for CVS files, or empty string if null.
    private static String formatDecimal(BigDecimal v) {
        return v == null ? "" : String.format("%.2f", v);
    }

    // Loader for goods classification CSV file. Used for printing friendly names.
    public static class GoodsClassificationLoader {
        public static Map<String, String> loadHs4Descriptions(Path path) throws IOException {
            Map<String, String> result = new HashMap<>();
            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                         .setHeader()
                         .setSkipHeaderRecord(true)
                         .setTrim(true)
                         .build()
                         .parse(reader)) {

                // For each row, pick HS4 code and description, store in map.
                for (CSVRecord record : parser) {
                    String hs4Code = record.get("NZHSC_Level_2_Code_HS4");
                    String description = record.get("NZHSC_Level_2");

                    // Only add valid HS4 codes (4 digits).
                    if (hs4Code != null && hs4Code.matches("\\d{4}")) {
                        result.put(hs4Code, description);
                    }
                }
            }
            return result;
        }
    }
}
