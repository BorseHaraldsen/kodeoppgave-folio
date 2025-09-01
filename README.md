# Kodeoppgave gjort til Folio.

---

Løst i Java med Apache Commons CSV.

Programmet analyserer handelsdata fra New Zealand og beregner:
- Handelsbalansen (eksport – import). Oppgaveteksten kan kanskje bli tolket som at det skal være import – eksport, 
men tok valget om å følge typisk handelsbalanse-definisjon.
- Det mest importerte produktet og verdien
- Det mest eksporterte produktet og verdien

Begrensninger:
- Kun år 2024 (`time_ref` starter med "2024")
- Kun varer av typen `"Goods"`
- Kun produkter med firesifret HS4-kode
- Verdi i Value kolonnen

Resultater genereres for:
- Norge
- Alle 27 EU-land individuelt
- EU som helhet

---

## Hvordan kjøre

Krav: **Java 17+** og **Maven.**



# Kjør (default input: output_csv_full.csv i samme mappe)  
mvn exec:java -Dexec.mainClass=org.tradereport.Main  

# Eller angi inputfil
mvn exec:java -Dexec.mainClass=org.tradereport.Main -Dexec.args="path/to/output_csv_full.csv">  

# Og for console output.txt
mvn exec:java -Dexec.mainClass=org.tradereport.Main > console_output.txt  

java -jar target/borse-kodeoppgave-folio-1.0-SNAPSHOT.jar > console_output.txt

# Jar
mvn clean package  
java -jar target/borse-kodeoppgave-folio-1.0-SNAPSHOT.jar  

# Potentially
mvn clean package shade:shade

# Test
mvn clean test