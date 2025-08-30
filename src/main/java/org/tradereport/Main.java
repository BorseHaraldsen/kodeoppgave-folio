package org.tradereport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;

// I chose CSV parser library to not reinvent the wheel and write in Java, avoid issues with things such as quoted fields. Commas easier but still.. Also lots of documentation :).
// Performs quite alright on large files, however straight BufferedReader or Pandas could maybe be faster in clean data or smaller datasets.

/**
 * Main class for analyzing trade data from CSV files.
 * Processes import/export data for Norway 2024, focusing on Goods with HS4 product codes.
 */
public class Main {

    // Country code for Norway - used to filter data to only Norwegian trade.
    private static final String NORWAY_ISO2 = "NO";      // keep logic simple for now

    // Year prefix for filtering to 2024 data only (format: YYYYMM in the dataset).
    private static final String YEAR_PREFIX = "2024";    // assignment scope

    // Default file path - should be overridden by command line arguments in production.
    // System property or environment variable.
    private static final String DEFAULT_FILE_PATH = System.getProperty("trade.data.path",
            System.getenv().getOrDefault("TRADE_DATA_PATH", "output_csv_full.csv"));

    // Default path for goods classification file
    private static final String DEFAULT_CLASS_PATH = System.getProperty("goods.classification.path",
            System.getenv().getOrDefault("GOODS_CLASS_PATH", "goods_classification.csv"));


    public static void main(String[] args) {

        //  Get the input path from args.
        if (args.length == 0) {
            System.out.println("Usage:");
            System.out.println("  mvn exec:java -Dexec.args=\"<path/to/output_csv_full.csv>\"");
            System.out.println("Or set environment variable TRADE_DATA_PATH");
            System.out.println("Or set system property -Dtrade.data.path=<path>");
        }

        // Use command line argument if provided, otherwise fall back to configurable default.
        String filePath = args.length > 0 ? args[0] : DEFAULT_FILE_PATH;

        // Convert string path to Path object for better file system operations.
        Path csvPath = Paths.get(filePath);

        // Error if file not found.
        if (!Files.exists(csvPath)) {
            System.err.println("File not found: " + csvPath.toAbsolutePath());
            return;
        }

        // Load HS4 descriptions into a map for print.
        Map<String, String> hs4Descriptions;
        try {
            hs4Descriptions = GoodsClassificationLoader.loadHs4Descriptions(Paths.get(DEFAULT_CLASS_PATH));
            System.out.println("Loaded " + hs4Descriptions.size() + " HS4 descriptions.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load goods_classification.csv", e);
        }

        // Variables to hold total import and export values for Norway 2024 (Goods, 4 digit HS4 codes.)
        // Using BigDecimal for precision, prevents floating point errors when summing monetary values.
        BigDecimal importsNO = BigDecimal.ZERO;
        BigDecimal exportsNO = BigDecimal.ZERO;

        // Maps that track total import and export values for each product code.
        // Key: HS4 product code (String because some codes might not be numbers).
        java.util.Map<String, BigDecimal> importByProductNO = new java.util.HashMap<>();
        java.util.Map<String, BigDecimal> exportByProductNO = new java.util.HashMap<>();

        // Using BigDecimal for accurate summation of money values in case of some data with many decimals.
        // However, double most likely fine too in this particular dataset.

        //  Open the file and create a streaming CSV parser.
        // Try-with-resources ensures both reader and parser are properly closed.
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()                   // read column names from first row.
                     .setSkipHeaderRecord(true)     // skip the header as a data row when iterating data.
                     .setTrim(true)                 // trim whitespace from around data.
                     .setIgnoreSurroundingSpaces(true) // Ignore spaces around data.
                     .build()
                     .parse(reader)) {

            //  Show the headers we detected.
            System.out.println("Headers detected: " + parser.getHeaderMap().keySet());

            //  Initializes counters for total rows, number of rows shown, and a variable to hold the last row.
            long total = 0;          // Total count of data rows processed.
            int shown = 0;           // Counter for sample rows displayed.
            CSVRecord last = null;   // Keep reference to last row for debugging.

            //  Iterate the rows, print the first 5 rows, and count total rows.
            for (CSVRecord r : parser) {
                total++;
                last = r; // keep overwriting to always have the last row.

                // Display first 5 rows as a sample of the data structure.
                if (shown < 5) {
                    String timeRef     = get(r, "time_ref");      // YYYYMM format date.
                    String account     = get(r, "account");       // "Imports" or "Exports".
                    String code        = get(r, "code");          // Product code (HS4).
                    String countryCode = get(r, "country_code");  // ISO2 country code.
                    String productType = get(r, "product_type");  // "Goods" or "Services".
                    String value       = get(r, "value");         // Monetary value.
                    //Optional field, may not be present in all files.
                    String status      = getOptional(r, "status"); // Optional status field.

                    // Print the values in a formatted way.
                    System.out.printf("%s | %s | %s | %s | %s | %s | %s%n",
                            timeRef, account, code, countryCode, productType, value, status);
                    // Increment the counter for rows displayed.
                    shown++;
                }

                // Filter and aggregate for Norway 2024, Goods, HS4, numeric value.

                // Extract all necessary fields for filtering.
                String timeRef     = get(r, "time_ref");
                String productType = get(r, "product_type");
                String countryCode = get(r, "country_code");
                String code        = get(r, "code");
                String account     = get(r, "account");
                String valueStr    = get(r, "value");

                // Filtering by Norway Country Code.
                if (!countryCode.equals(NORWAY_ISO2)) continue;

                // Check if this row is from year 2024 (timeRef starts with "2024").
                if (!isYear2024(timeRef)) continue;

                // Check if this is Goods data (not Services).
                if (!isGoods(productType)) continue;

                // Check if the product code is exactly 4 digits (HS4 classification).
                if (!isHs4(code)) continue;

                // Try to parse the value as a decimal number.
                // Returns null if not a valid number, allowing us to skip non-numeric values.
                BigDecimal v = parseDecimalOrNull(valueStr);
                if (v == null) continue;                      // Skip rows with non-numeric values.
                // NOTE: do NOT skip v == 0.0; zeros are legitimate values.

                // Check the value based on whether it's import or export.
                if (isImport(account)) {
                    // Add to total imports.
                    importsNO = importsNO.add(v);
                    // Add to product-specific imports (merge handles both new and existing keys).
                    // This is the map we made earlier in code.
                    // Will then later be used to check max value when we're done.
                    importByProductNO.merge(code, v, BigDecimal::add);
                } else if (isExport(account)) {
                    // Add to total exports
                    // Same as with imports.
                    exportsNO = exportsNO.add(v);
                    // Add to product-specific exports (merge handles both new and existing keys)
                    // Same as with imports.
                    exportByProductNO.merge(code, v, BigDecimal::add);
                }
                // Rows that are neither Import nor Export are silently skipped
            }

            // Print the last row too, to see if anything odd at end of file.
            System.out.println("Last row seen: " + last);
            //  Total rows (data lines) for sanity.
            System.out.println("Total data rows read: " + total);

            // --- Results for Norway 2024 (Goods, HS4) ---
            System.out.println("\n=== Norway 2024 (Goods, HS4) ===");

            System.out.printf("Total Imports: %.2f%n", importsNO);
            System.out.printf("Total Exports: %.2f%n", exportsNO);

            BigDecimal tradeBalance = exportsNO.subtract(importsNO);
            System.out.printf("Trade balance (Exports - Imports): %.2f%n", tradeBalance);

            // Find the product with maximum import value.
            // I made map, then filled it with total imports, then here check the max one.
            java.util.Map.Entry<String, BigDecimal> topImp = maxEntry(importByProductNO);
            // Find the product with maximum export value
            // I made map, then filled it with total exports, then here check the max one.
            java.util.Map.Entry<String, BigDecimal> topExp = maxEntry(exportByProductNO);



            // Display top imported product or n/a if no data
            if (topImp != null) {
                String desc = hs4Descriptions.getOrDefault(topImp.getKey(), "(unknown)");
                System.out.printf("Most imported HS4: %s - %s (%.2f)%n", topImp.getKey(), desc, topImp.getValue());
            } else {
                System.out.println("Most imported HS4: n/a");
            }

            // Display top exported product or n/a if no data
            if (topExp != null) {
                String desc = hs4Descriptions.getOrDefault(topExp.getKey(), "(unknown)");
                System.out.printf("Most exported HS4: %s - %s (%.2f)%n", topExp.getKey(), desc, topExp.getValue());
            } else {
                System.out.println("Most exported HS4: n/a");
            }

            System.out.println("(Step 3 done: Norway-only numbers computed correctly.)");

        } catch (IOException e) {
            // Convert IOException to RuntimeException for simpler error handling
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a required field from a CSV record.
     * @param r The CSV record to read from
     * @param name The column name to retrieve
     * @return The value in that column
     * @throws IllegalStateException if the column doesn't exist
     */
    private static String get(CSVRecord r, String name) {
        try {
            return r.get(name);
        } catch (IllegalArgumentException e) {
            // If column is missing, throw a more descriptive error
            throw new IllegalStateException("Missing expected column: " + name, e);
        }
    }

    /**
     * Gets an optional field from a CSV record.
     * @param r The CSV record to read from
     * @param name The column name to retrieve
     * @return The value in that column, or empty string if column doesn't exist
     */
    private static String getOptional(CSVRecord r, String name) {
        try {
            return r.get(name);
        } catch (IllegalArgumentException e) {
            return ""; // optional field not present - return empty string instead of failing
        }
    }

    // --- Helper methods for data filtering and validation ---

    /**
     * Checks if the time reference is from year 2024.
     * @param timeRef Time reference in YYYYMM format (e.g., "202401" for January 2024)
     * @return true if the time reference starts with "2024"
     */
    private static boolean isYear2024(String timeRef) {
        if (timeRef == null) return false;
        // dataset uses YYYYMM (e.g., 202401..202412)
        return timeRef.startsWith(YEAR_PREFIX);
    }

    /**
     * Checks if the product type is "Goods" (case-insensitive).
     * @param productType The product type string
     * @return true if productType is "Goods" (ignoring case and whitespace)
     */
    private static boolean isGoods(String productType) {
        return productType != null && productType.trim().equalsIgnoreCase("Goods");
    }

    /**
     * Checks if the code is a valid HS4 code (exactly 4 digits).
     * @param code The product code to validate
     * @return true if code consists of exactly 4 digits
     */
    private static boolean isHs4(String code) {
        // Regular expression: \\d{4} means exactly 4 digits (0-9)
        return code != null && code.matches("\\d{4}");
    }

    /**
     * Checks if the account type is "Imports" (case-insensitive).
     * @param account The account type string
     * @return true if account is "Imports" (ignoring case and whitespace)
     */
    private static boolean isImport(String account) {
        return account != null && account.trim().equalsIgnoreCase("Imports");
    }

    /**
     * Checks if the account type is "Exports" (case-insensitive).
     * @param account The account type string
     * @return true if account is "Exports" (ignoring case and whitespace)
     */
    private static boolean isExport(String account) {
        return account != null && account.trim().equalsIgnoreCase("Exports");
    }

    /**
     * Parses a string to BigDecimal, returning null if parsing fails.
     * @param s The string to parse
     * @return BigDecimal value or null if not a valid number
     */
    // Important. Should not include the ones without value in the "value" - column as mentioned in the task.
    private static BigDecimal parseDecimalOrNull(String s) {
        // Return null for null or blank strings
        if (s == null || s.isBlank()) return null;
        try {
            // Dataset uses dot as decimal separator, but just in case: remove commas (thousands separators).
            // This handles formats like "1,234.56" -> "1234.56"
            return new BigDecimal(s.replace(",", ""));
        } catch (NumberFormatException e) {
            // Return null if the string can't be parsed as a number
            return null;
        }
    }

    /**
     * Finds the map entry with the maximum value.
     * @param map Map of product codes to trade values
     * @return Entry with maximum value, or null if map is empty
     */
    private static java.util.Map.Entry<String, BigDecimal> maxEntry(java.util.Map<String, BigDecimal> map) {
        if (map.isEmpty()) return null;
        // Use Java 8 streams to find the entry with maximum value
        // comparingByValue() compares BigDecimal values naturally
        return map.entrySet().stream()
                .max(java.util.Map.Entry.comparingByValue())
                .orElse(null);
    }

    /**
     * Utility class to load HS4 product code descriptions from goods_classification.csv.
     * Creates a map from code (e.g., "7601") to human-readable description (e.g., "Aluminium; unwrought").
     */
    public static class GoodsClassificationLoader {

        /**
         * Loads HS4 descriptions into a map.
         * @param path Path to goods_classification.csv
         * @return Map of HS4 code -> description
         * @throws IOException if file cannot be read
         */
        public static Map<String, String> loadHs4Descriptions(Path path) throws IOException {
            Map<String, String> result = new HashMap<>();

            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                         .setHeader()                   // read headers
                         .setSkipHeaderRecord(true)     // skip header row
                         .setTrim(true)
                         .build()
                         .parse(reader)) {

                for (CSVRecord record : parser) {
                    String hs4Code = record.get("NZHSC_Level_2_Code_HS4");
                    String description = record.get("NZHSC_Level_2"); // the readable description
                    if (hs4Code != null && hs4Code.matches("\\d{4}")) {
                        result.put(hs4Code, description);
                    }
                }
            }

            return result;
        }
    }

    // If I end up working with this for future, but more manual without dependencies.
    /*
    class SimpleCsvReader {
        // Alternative implementation without external dependencies
        // Kept for reference but not recommended for production use
        // as it doesn't handle quoted fields, escaped characters, etc.

        public static void read(String path, Consumer<String[]> consumer) throws IOException {
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String headerLine = br.readLine(); // skip header
                if (headerLine == null) return;

                String line;
                while ((line = br.readLine()) != null) {
                    // Very naive split — works if no commas inside quotes
                    String[] cols = line.split(",", -1); // -1 keeps empty strings
                    consumer.accept(cols);
                }
            }
        }
    }
    */
}