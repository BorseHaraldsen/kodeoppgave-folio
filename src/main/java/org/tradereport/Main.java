package org.tradereport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

// I chose CSV parser library to not reinvent the wheel, avoid issues with things such as quoted fields. Commas easier but still.. Also lots of documentation :).
// Performs quite alright on large files, however straight BufferedReader could maybe be faster in clean data.

public class Main {
    public static void main(String[] args) {

        //  Get the input path from args.
        if (args.length == 0) {
            System.out.println("Usage:");
            System.out.println("  mvn exec:java -Dexec.args=\"<D:/PersonalProjects/borse-kodeoppgave-folio/output_csv_full.csv>\"");
            return;
        }

        //Hardcode path for easier testing.
        String filePath = "D:/PersonalProjects/borse-kodeoppgave-folio/output_csv_full.csv";

        Path csvPath = Paths.get(filePath);

        //Error if file not found.
        if (!Files.exists(csvPath)) {
            System.err.println("File not found: " + csvPath.toAbsolutePath());
            return;
        }

        //  Open the file and create a streaming CSV parser.
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()                   // read column names from first row.
                     .setSkipHeaderRecord(true)     // skip the header as a data row when iterating data.
                     .setTrim(true)                 // trim whitespace from around data.
                     .setIgnoreSurroundingSpaces(true) //Ignore spaces around data.
                     .build()
                     .parse(reader)) {

            //  Show the headers we detected.
            System.out.println("Headers detected: " + parser.getHeaderMap().keySet());

            //  Initializes counters for total rows, number of rows shown, and a variable to hold the last row.
            long total = 0;
            int shown = 0;
            CSVRecord last = null;
            //  Iterate the rows, print the first 5 rows, and count total rows. I usually use for i loops but it stores it in a List so in memory, not ideal.
            for (CSVRecord r : parser) {
                total++;
                last = r; // keep overwriting.
                if (shown < 5) {
                    // Gets the values from each column.
                    String timeRef     = get(r, "time_ref");
                    String account     = get(r, "account");
                    String code        = get(r, "code");
                    String countryCode = get(r, "country_code");
                    String productType = get(r, "product_type");
                    String value       = get(r, "value");
                    //Optional field, may not be present in all files.
                    String status      = getOptional(r, "status");

                    // Print the values in a formatted way.
                    System.out.printf("%s | %s | %s | %s | %s | %s | %s%n",
                            timeRef, account, code, countryCode, productType, value, status);
                    // Increment the counter for rows displayed.
                    shown++;
                }
            }
            // Print the last row too, to see if anything odd at end of file.
            System.out.println("Last row seen: " + last);
            //  Total rows (data lines) for sanity.
            System.out.println("Total data rows read: " + total);
            System.out.println("(Step 1 done: we can read and iterate the CSV.)");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String get(CSVRecord r, String name) {
        try {
            return r.get(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Missing expected column: " + name, e);
        }
    }

    private static String getOptional(CSVRecord r, String name) {
        try {
            return r.get(name);
        } catch (IllegalArgumentException e) {
            return ""; // optional field not present
        }
    }


    // If I end up working with this for future, but more manual without dependencies.

        /*
        class SimpleCsvReader {


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
        * */





}