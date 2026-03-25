package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.mcw.rgd.dao.spring.IntListQuery;

import java.sql.*;
import java.util.*;

/**
 * Removes duplicate rows from the GENE_LOCI table for a given map_key.
 * For each (map_key, chromosome, pos, gene_symbols) group, keeps one row and deletes the rest.
 * <p>
 * Two strategies are available:
 * <ul>
 *   <li>{@code usePositionScan=false} (default) — uses a NOT IN subquery per chromosome.
 *       Efficient for moderate table sizes.</li>
 *   <li>{@code usePositionScan=true} — retrieves distinct positions first, then scans
 *       rows per position. Optimized for very large tables (1B+ rows) where the
 *       subquery approach is too slow.</li>
 * </ul>
 */
public class CleanDuplicates {

    Logger log = LogManager.getLogger("core");
    Logger logDeleted = LogManager.getLogger("deleted");

    MapDAO dao = new MapDAO();

    private String version;
    private boolean usePositionScan = false;

    public void run(int mapKey) throws Exception {

        log.info(version);
        log.info("   " + dao.getConnectionInfo());
        log.info("   cleanDuplicates for map_key=" + mapKey + "  usePositionScan=" + usePositionScan);

        long time1 = System.currentTimeMillis();

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        Map<String, Integer> chrSizes = dao.getChromosomeSizes(mapKey);
        List<String> chromosomes = new ArrayList<>(chrSizes.keySet());
        Collections.shuffle(chromosomes);

        int totalDeleted = 0;

        for (String chromosome : chromosomes) {
            int chrDeleted = usePositionScan
                    ? cleanChromosomeByPosition(mapKey, chromosome)
                    : cleanChromosomeBySubquery(mapKey, chromosome);

            if (chrDeleted > 0) {
                log.info("   chr " + chromosome + ": deleted " + Utils.formatThousands(chrDeleted) + " duplicates");
            }
            log.info("   " + memoryMonitor.getSummary());
            totalDeleted += chrDeleted;
        }

        memoryMonitor.stop();

        long time2 = System.currentTimeMillis();
        log.info("cleanDuplicates for map_key=" + mapKey + ": total deleted " + Utils.formatThousands(totalDeleted)
                + ";  " + Utils.formatElapsedTime(time1, time2));
        log.info("");
    }

    /**
     * Original approach: uses a NOT IN subquery to find duplicate rowids per chromosome.
     */
    int cleanChromosomeBySubquery(int mapKey, String chromosome) throws Exception {

        String sqlSelect = """
            SELECT rowid, map_key, chromosome, pos, gene_symbols, gene_symbols_lc
            FROM gene_loci
            WHERE map_key=? AND chromosome=? AND rowid NOT IN(
                SELECT MIN(rowid)
                FROM gene_loci
                WHERE map_key=? AND chromosome=?
                GROUP BY map_key, chromosome, pos, gene_symbols
            )
            """;

        String sqlDelete = """
            DELETE FROM gene_loci WHERE rowid=?
            """;

        int chrDeleted = 0;

        try (Connection conn = dao.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sqlSelect);
            ps.setInt(1, mapKey);
            ps.setString(2, chromosome);
            ps.setInt(3, mapKey);
            ps.setString(4, chromosome);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String rowid = rs.getString(1);
                int pos = rs.getInt(4);
                String geneSymbols = rs.getString(5);

                logDeleted.debug("DUPLICATE: map_key=" + mapKey + "|chr=" + chromosome
                        + "|pos=" + pos + "|gene_symbols=" + geneSymbols);

                try (PreparedStatement psDel = conn.prepareStatement(sqlDelete)) {
                    psDel.setString(1, rowid);
                    psDel.executeUpdate();
                }
                chrDeleted++;
            }
        }

        return chrDeleted;
    }

    /**
     * Position-scan approach: retrieves distinct positions first, then for each position
     * reads all rows and detects duplicates in memory. Optimized for very large tables.
     */
    int cleanChromosomeByPosition(int mapKey, String chromosome) throws Exception {

        String sqlPositions = """
            SELECT DISTINCT pos FROM gene_loci
            WHERE map_key=? AND chromosome=?
            ORDER BY pos
            """;

        List<Integer> positions = IntListQuery.execute(dao, sqlPositions, mapKey, chromosome);
        log.debug("   chr " + chromosome + ": " + Utils.formatThousands(positions.size()) + " distinct positions");

        String sqlRows = """
            SELECT rowid, gene_symbols FROM gene_loci
            WHERE map_key=? AND chromosome=? AND pos=?
            ORDER BY rowid
            """;

        String sqlDelete = """
            DELETE FROM gene_loci WHERE rowid=?
            """;

        int chrDeleted = 0;

        try (Connection conn = dao.getConnection()) {
            PreparedStatement psRows = conn.prepareStatement(sqlRows);
            PreparedStatement psDel = conn.prepareStatement(sqlDelete);

            for (int pos : positions) {
                psRows.setInt(1, mapKey);
                psRows.setString(2, chromosome);
                psRows.setInt(3, pos);

                // collect all rows for this pos; key: gene_symbols, value: list of rowids
                Map<String, List<String>> symbolToRowids = new HashMap<>();
                ResultSet rs = psRows.executeQuery();
                while (rs.next()) {
                    String rowid = rs.getString(1);
                    String geneSymbols = rs.getString(2);
                    symbolToRowids.computeIfAbsent(geneSymbols, k -> new ArrayList<>()).add(rowid);
                }

                // keep the first rowid per gene_symbols group, delete the rest
                for (Map.Entry<String, List<String>> entry : symbolToRowids.entrySet()) {
                    List<String> rowids = entry.getValue();
                    if (rowids.size() <= 1) {
                        continue;
                    }
                    String geneSymbols = entry.getKey();
                    for (int i = 1; i < rowids.size(); i++) {
                        psDel.setString(1, rowids.get(i));
                        psDel.executeUpdate();
                        chrDeleted++;

                        logDeleted.debug("DUPLICATE: map_key=" + mapKey + "|chr=" + chromosome
                                + "|pos=" + pos + "|gene_symbols=" + geneSymbols);
                    }
                }
            }
        }

        return chrDeleted;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setUsePositionScan(boolean usePositionScan) {
        this.usePositionScan = usePositionScan;
    }
}
