package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.Map;

/**
 * @author jdepons
 * @since 4/26/12
 */
public class GeneLociPipeline {

    Logger log = LogManager.getLogger("core");
    Logger logDuplicates = LogManager.getLogger("duplicates");
    Logger logInserted = LogManager.getLogger("inserted");
    Logger logDeleted = LogManager.getLogger("deleted");

    private String version;

    MapDAO dao = new MapDAO();
    private List<RunInfo> runList;
    private int deleteCount;
    private int duplicateCount;
    private int insertCount;
    private int unchangedCount;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        GeneLociPipeline importer = (GeneLociPipeline) (bf.getBean("importer"));

        String mapKeyStr = null;
        boolean cleanDuplicates = false;
        for( String arg: args ) {
            if( arg.startsWith("--mapKey=") ) {
                mapKeyStr = arg.substring(9);
            } else if( arg.equals("--cleanDuplicates") ) {
                cleanDuplicates = true;
            }
        }

        if( cleanDuplicates ) {
            if( mapKeyStr == null ) {
                throw new Exception("--mapKey is mandatory when using --cleanDuplicates");
            }
            CleanDuplicates cleaner = (CleanDuplicates) (bf.getBean("cleanDuplicates"));
            cleaner.setVersion(importer.getVersion());

            if( mapKeyStr.equalsIgnoreCase("all") ) {
                for( RunInfo info: importer.getRunList() ) {
                    if( info.isRunIt() ) {
                        cleaner.run(info.getMapKey());
                    }
                }
            } else {
                cleaner.run(Integer.parseInt(mapKeyStr));
            }
        } else {
            int mapKey = mapKeyStr != null ? Integer.parseInt(mapKeyStr) : 0;
            importer.run(mapKey);
        }
    }

    public void run(int mapKey) throws Exception {
        log.info(getVersion());
        long time1 = System.currentTimeMillis();

        log.info("   "+dao.getConnectionInfo());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info("   started at "+sdt.format(new Date(time1)));

        for( RunInfo info: this.getRunList() ) {
            if( info.isRunIt() ) {
                if( mapKey==0 || info.getMapKey()==mapKey ) {
                    run(info);
                }
            }
        }

        long time2 = System.currentTimeMillis();
        log.info("gene loci pipeline complete: " + Utils.formatElapsedTime(time1, time2));
        log.info("====");
    }

    public void run(RunInfo info) throws Exception {

        String speciesName = SpeciesType.getCommonName(info.getSpeciesTypeKey());
        log.info(speciesName+" START   map_key="+info.getMapKey());

        deleteCount = 0;
        duplicateCount = 0;
        insertCount = 0;
        unchangedCount = 0;

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        long time1 = System.currentTimeMillis();

        Map<String,Integer> chrSizes = dao.getChromosomeSizes(info.getMapKey());

        // randomize chromosomes processed
        List<String> chromosomes = new ArrayList<>(chrSizes.keySet());
        Collections.shuffle(chromosomes);

        for( String chromosome: chromosomes ) {

            long timeUpdatePos1 = System.currentTimeMillis();

            // load existing gene_loci data for this chromosome
            insertedKeys.clear();
            Set<String> existingLoci = loadExistingLoci(info.getMapKey(), chromosome);

            // compute new loci
            List<GeneData> geneDatas = processChromosome(chromosome, info.getMapKey(), info.getSpeciesTypeKey());
            int gdi = 0;

            Set<Integer> variantPositions = getPosForVariants(chromosome, info.getMapKey());

            List<GeneLocus> loci = new ArrayList<>();
            List<GeneData> genesInFrame = new ArrayList<>();
            String lastRemovedGeneSymbol = "";
            int chrLength = chrSizes.get(chromosome)+1000000;
            for( int pos=1; pos<=chrLength; pos++ ) {

                // check if any genes are going out of frame
                for( int i=genesInFrame.size()-1; i>=0; i-- ) {
                    GeneData data = genesInFrame.get(i);
                    if( data.stopPos < pos ) {
                        lastRemovedGeneSymbol = data.geneSymbol;
                        genesInFrame.remove(i); // gene going out of frame
                    }
                }

                // check if there are any new genes going into frame
                String addedGeneSymbols = null;
                while( gdi<geneDatas.size() ) {
                    GeneData data = geneDatas.get(gdi);
                    if( data.startPos <= pos ) {
                        if( addedGeneSymbols==null )
                            addedGeneSymbols = data.geneSymbol;
                        else
                            addedGeneSymbols += "*" + data.geneSymbol;

                        genesInFrame.add(data);
                        gdi++;
                    }
                    else
                        break;
                }

                // if there are added gene symbols, update intergenic loci
                // and write loci to database
                if( addedGeneSymbols!=null ) {
                    for( GeneLocus locus: loci ) {
                        if( locus.genicStatus.equals("intergenic") )
                            locus.geneSymbols += "|" + addedGeneSymbols;
                    }
                    collectLoci(loci);
                }

                // write gene loc entry
                if( variantPositions.contains(pos) ) {
                    GeneLocus locus = new GeneLocus();
                    locus.pos = pos;
                    locus.chromosome = chromosome;
                    locus.mapKey = info.getMapKey();
                    locus.genicStatus = "intergenic";
                    for( GeneData data: genesInFrame ) {
                        if( locus.geneSymbols==null )
                            locus.geneSymbols = data.geneSymbol;
                        else
                            locus.geneSymbols += "*" + data.geneSymbol;
                        locus.genicStatus = "genic";
                    }
                    if( locus.genicStatus.equals("intergenic") ) {
                        // write previous gene symbol
                        locus.geneSymbols = lastRemovedGeneSymbol;
                    }
                    loci.add(locus);
                }
            }

            // update any intergenic loci
            // and write loci to database
            for( GeneLocus locus: loci ) {
                if( locus.genicStatus.equals("intergenic") )
                    locus.geneSymbols += "|";
            }
            collectLoci(loci);
            int insertedBefore = insertCount;
            int unchangedBefore = unchangedCount;
            flushLoci(existingLoci);

            // delete obsolete rows that remain in existingLoci (not matched by new data)
            int deleted = deleteObsoleteLoci(info.getMapKey(), chromosome, existingLoci);
            deleteCount += deleted;

            long timeUpdatePos2 = System.currentTimeMillis();
            log.debug(" complete CHR "+chromosome
                    +";  inserted="+Utils.formatThousands(insertCount - insertedBefore)
                    +";  unchanged="+Utils.formatThousands(unchangedCount - unchangedBefore)
                    +";  deleted="+Utils.formatThousands(deleted)
                    +";  "+Utils.formatElapsedTime(timeUpdatePos1, timeUpdatePos2));
            log.debug("   "+memoryMonitor.getSummary());
        }

        long time2 = System.currentTimeMillis();
        log.info(speciesName+" map_key="+info.getMapKey()+"  rows inserted: "+Utils.formatThousands(insertCount)
                +"  unchanged: "+Utils.formatThousands(unchangedCount)
                +"  deleted: "+Utils.formatThousands(deleteCount));
        if( duplicateCount > 0 ) {
            log.info(speciesName+" duplicates skipped for map_key="+info.getMapKey()+": "+Utils.formatThousands(duplicateCount));
        }
        memoryMonitor.stop();
        log.info(memoryMonitor.getSummary());
        log.info(speciesName+" Finished import "+"; "+Utils.formatElapsedTime(time1, time2));
        log.info("");
    }

    public List<GeneData> processChromosome(String chr, int mapKey, int speciesType) throws Exception {

        String sql = """
            SELECT m.rgd_id,m.start_pos,m.stop_pos,g.gene_symbol,g.gene_type_lc
            FROM genes g,rgd_ids r,maps_data m
            WHERE g.rgd_id=r.rgd_id AND r.object_key=1 AND r.object_status='ACTIVE' AND r.species_type_key=?
              AND m.rgd_id=g.rgd_id AND m.map_key=? AND chromosome=?
            ORDER BY m.start_pos
            """;

        List<GeneData> geneDatas = new ArrayList<>();

        try(Connection conn = dao.getConnection()) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, speciesType);
            ps.setInt(2, mapKey);
            ps.setString(3, chr);

            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {
                GeneData data = new GeneData();
                data.geneRgdId = rs.getInt(1);
                data.geneSymbol = rs.getString(4);
                data.startPos = rs.getInt(2);
                data.stopPos = rs.getInt(3);
                geneDatas.add(data);
            }
        }
        log.debug("   loaded gene positions on chr "+chr+" ="+geneDatas.size());

        return geneDatas;
    }

    Set<Integer> getPosForVariants(String chr, int mapKey) throws Exception {

        String sql = """
            SELECT start_pos FROM variant_map_data
            WHERE chromosome=? AND map_key=?
            """;

        Set<Integer> set = new HashSet<>();
        DataSource ds = DataSourceFactory.getInstance().getCarpeNovoDataSource();

        try(Connection conn = ds.getConnection()) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, chr);
            ps.setInt(2, mapKey);

            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {
                set.add(rs.getInt(1));
            }
        }
        return set;
    }

    /**
     * Load existing gene_loci rows for given map_key and chromosome.
     * Returns a set of "pos|gene_symbols_lc" keys.
     */
    Set<String> loadExistingLoci(int mapKey, String chromosome) throws Exception {

        String sql = """
            SELECT pos, gene_symbols_lc FROM gene_loci
            WHERE map_key=? AND chromosome=?
            """;

        Set<String> set = new HashSet<>();

        try(Connection conn = dao.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, mapKey);
            ps.setString(2, chromosome);

            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {
                int pos = rs.getInt(1);
                String geneSymbolsLc = rs.getString(2);
                set.add(pos + "|" + geneSymbolsLc);
            }
        }
        log.debug("   loaded existing loci for chr "+chromosome+": "+set.size());
        return set;
    }

    List<GeneLocus> loci2 = new ArrayList<>(100000);
    Set<String> insertedKeys = new HashSet<>();

    /**
     * Collect loci into buffer, expanding '*' entries into separate rows.
     */
    void collectLoci(List<GeneLocus> loci) throws CloneNotSupportedException {

        for( GeneLocus locus: loci ) {

            if( !locus.geneSymbols.contains("*") ) {
                loci2.add(locus);
            }
            else {
                // CASE1: genic
                // RCC1*SNHG3*SNHG3-RCC1
                if( locus.genicStatus.equals("genic") ) {

                    expandSymbolsInLocus(locus, locus.geneSymbols, "", "");
                }
                else {
                    // CASE2: intergenic - star after bar
                    // HOXC8|HOXC4*HOXC6*HOXC5
                    int barPos = locus.geneSymbols.indexOf('|');
                    int starPos = locus.geneSymbols.indexOf('*');
                    if( starPos > barPos ) {
                        String prefix = locus.geneSymbols.substring(0, barPos+1);
                        expandSymbolsInLocus(locus, locus.geneSymbols.substring(barPos+1), prefix, "");
                    }
                    else {
                    // CASE3: intergenic - star before bar
                    // HOXC4*HOXC6*HOXC5|HOXC8
                        String suffix = locus.geneSymbols.substring(barPos);
                        expandSymbolsInLocus(locus, locus.geneSymbols.substring(0, barPos), "", suffix);
                    }
                }
            }
        }

        loci.clear();
    }

    void expandSymbolsInLocus(GeneLocus locus, String geneSymbols, String prefix, String suffix) throws CloneNotSupportedException {

        String[] symbols = geneSymbols.split("[*]");

        // add first locus
        locus.geneSymbols = prefix + symbols[0] + suffix;
        loci2.add(locus);

        for( int i=1; i<symbols.length; i++ ) {
            GeneLocus locusClone = (GeneLocus) locus.clone();
            locusClone.geneSymbols = prefix + symbols[i] + suffix;
            loci2.add(locusClone);
        }
    }

    /**
     * Flush buffered loci: QC against existing data, insert only new rows.
     * Rows matched in existingLoci are removed from it (so what remains is obsolete).
     */
    void flushLoci(Set<String> existingLoci) throws Exception {

        String sqlInsert = """
            INSERT INTO gene_loci (genic_status, gene_symbols, gene_symbols_lc, map_key, chromosome, pos)
            VALUES(?, ?, ?, ?, ?, ?)
            """;

        for( GeneLocus locus: loci2 ) {

            // check for duplicates within the same run first
            String key = locus.mapKey + "|" + locus.chromosome + "|" + locus.pos + "|" + locus.geneSymbols;
            if( !insertedKeys.add(key) ) {
                duplicateCount++;
                logDuplicates.debug("DUPLICATE: " + key + "|" + locus.genicStatus);
                continue;
            }

            // check if this locus already exists in the database
            String existingKey = locus.pos + "|" + locus.geneSymbols.toLowerCase();
            if( existingLoci.remove(existingKey) ) {
                unchangedCount++;
                continue;
            }

            dao.update(sqlInsert, locus.genicStatus, locus.geneSymbols, locus.geneSymbols.toLowerCase(),
                    locus.mapKey, locus.chromosome, locus.pos);
            insertCount++;
            logInserted.debug(locus.mapKey + "|" + locus.chromosome + "|" + locus.pos + "|" + locus.genicStatus + "|" + locus.geneSymbols);
        }

        loci2.clear();
    }

    /**
     * Delete obsolete rows that were not matched by new data.
     * @param existingLoci remaining keys (pos|gene_symbols_lc) not seen in new data
     */
    int deleteObsoleteLoci(int mapKey, String chromosome, Set<String> existingLoci) throws Exception {

        if( existingLoci.isEmpty() ) {
            return 0;
        }

        String sqlDelete = """
            DELETE FROM gene_loci
            WHERE map_key=? AND chromosome=? AND pos=? AND gene_symbols_lc=?
            """;

        int deleted = 0;
        for( String key: existingLoci ) {
            int barPos = key.indexOf('|');
            int pos = Integer.parseInt(key.substring(0, barPos));
            String geneSymbolsLc = key.substring(barPos + 1);

            dao.update(sqlDelete, mapKey, chromosome, pos, geneSymbolsLc);
            deleted++;
            logDeleted.debug("OBSOLETE: " + mapKey + "|" + chromosome + "|" + pos + "|" + geneSymbolsLc);
        }
        return deleted;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setRunList(List<RunInfo> runList) {
        this.runList = runList;
    }

    public List<RunInfo> getRunList() {
        return runList;
    }

    class GeneData {
        public int geneRgdId;
        public String geneSymbol;
        public int startPos;
        public int stopPos;
    }

    class GeneLocus implements Cloneable {
        public int mapKey;
        public String chromosome;
        public int pos;
        public String genicStatus;
        public String geneSymbols;

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }
}
