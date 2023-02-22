package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.object.BatchSqlUpdate;

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

    private String version;

    MapDAO dao = new MapDAO();
    private List<RunInfo> runList;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        GeneLociPipeline importer = (GeneLociPipeline) (bf.getBean("importer"));

        int mapKey = 0;
        if( args.length>0 ) {
            if( args[0].startsWith("--mapKey=") ) {
                mapKey = Integer.parseInt(args[0].substring(9));
            }
        }
        importer.run(mapKey);
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

        initLociForVariants(info.getMapKey());

        // we do not create positions in GENE_LOCI fro DB_SNP for human, because that will be immensely huge
        if( info.getSpeciesTypeKey()==SpeciesType.RAT ) {
            initLociForDbSnpVariants(info.getMapKey(), info.getDbSnpBuild());
        }
        removeDuplicateLoci(info.getMapKey());

        long time1 = System.currentTimeMillis();

        Map<String,Integer> chrSizes = dao.getChromosomeSizes(info.getMapKey());

        // randomize chromosomes processed
        List<String> chromosomes = new ArrayList<>(chrSizes.keySet());
        Collections.shuffle(chromosomes);

        for( String chromosome: chromosomes ) {

            long timeUpdatePos1 = System.currentTimeMillis();

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
                    writeLoci(loci);
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

                if( pos%1000000==0 )
                    log.debug("processing chr"+chromosome+" at pos "+pos);
            }

            // update any intergenic loci
            // and write loci to database
            for( GeneLocus locus: loci ) {
                if( locus.genicStatus.equals("intergenic") )
                    locus.geneSymbols += "|";
            }
            writeLoci(loci);
            finishWriteLoci();

            long timeUpdatePos2 = System.currentTimeMillis();
            log.debug(" complete CHR "+chromosome+";  "+Utils.formatElapsedTime(timeUpdatePos1, timeUpdatePos2));
        }

        long time2 = System.currentTimeMillis();
        log.info(speciesName+" Finished import "+"; "+Utils.formatElapsedTime(time1, time2));
        log.info("");
    }

    public List<GeneData> processChromosome(String chr, int mapKey, int speciesType) throws Exception {

        long time1 = System.currentTimeMillis();

        String sql =
        "select m.rgd_id,m.start_pos,m.stop_pos,g.gene_symbol,g.gene_type_lc "+
        "from genes g,rgd_ids r,maps_data m "+
        "where g.rgd_id=r.rgd_id and r.object_key=1 and r.object_status='ACTIVE' and r.species_type_key=? "+
        "and m.rgd_id=g.rgd_id and m.map_key=? and chromosome=? "+
        "order by m.start_pos";

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
        long time2 = System.currentTimeMillis();
        log.debug("   loaded gene positions on chr "+chr+"; "+Utils.formatElapsedTime(time1, time2));

        return geneDatas;
    }

    Set<Integer> getPosForVariants(String chr, int mapKey) throws Exception {

        String sql = "SELECT pos FROM gene_loci WHERE chromosome=? AND map_key=?";

        Set<Integer> set = new HashSet<>();

        try(Connection conn = dao.getConnection()) {

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

    List<GeneLocus> loci2 = new ArrayList<>(100000);

    void writeLociInBulk(List<GeneLocus> loci) throws Exception {

        String sql1 =
        "UPDATE gene_loci SET genic_status=?,gene_symbols=?,gene_symbols_lc=? WHERE map_key=? AND chromosome=? AND pos=? ";

        BatchSqlUpdate su1 = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(), sql1,
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER});
        su1.compile();


        String sql2 =
        "INSERT INTO gene_loci (genic_status,gene_symbols,gene_symbols_lc,map_key,chromosome,pos) "+
        "VALUES(?,?,?,?,?,?)";

        BatchSqlUpdate su2 = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(), sql2,
                new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.INTEGER});
        su2.compile();

        for( GeneLocus locus: loci ) {
            if( locus.insertFlag )
                su2.update(locus.genicStatus, locus.geneSymbols, locus.geneSymbols.toLowerCase(), locus.mapKey, locus.chromosome, locus.pos);
            else
                su1.update(locus.genicStatus, locus.geneSymbols, locus.geneSymbols.toLowerCase(), locus.mapKey, locus.chromosome, locus.pos);
            //System.out.println("WRITE "+locus.mapKey+"|"+locus.chromosome+"|"+locus.pos+"|"+locus.genicStatus+"|"+locus.geneSymbols);
        }
        dao.executeBatch(su1);
        dao.executeBatch(su2);

        loci.clear();
    }

    void writeLoci(List<GeneLocus> loci) throws Exception {

        // add all loci to loci2 table expanding every '*' entry
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

        if( loci2.size()>50000 ) {
            writeLociInBulk(loci2);
        }
    }

    void expandSymbolsInLocus(GeneLocus locus, String geneSymbols, String prefix, String suffix) throws CloneNotSupportedException {

        String[] symbols = geneSymbols.split("[*]");

        // add first locus
        locus.geneSymbols = prefix + symbols[0] + suffix;
        loci2.add(locus);

        for( int i=1; i<symbols.length; i++ ) {
            GeneLocus locusClone = (GeneLocus) locus.clone();
            locusClone.geneSymbols = prefix + symbols[i] + suffix;
            locusClone.insertFlag = true;
            loci2.add(locusClone);
        }
    }

    void finishWriteLoci() throws Exception {
        writeLociInBulk(loci2);
    }

    void initLociForVariants(int mapKey) throws Exception {

        long time0 = System.currentTimeMillis();
        log.info("  initializing loci for variants");

        String sql = "INSERT INTO gene_loci (map_key,chromosome,pos) VALUES(?,?,?)";

        BatchSqlUpdate su = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(), sql,
                new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER});
        su.compile();

        Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();

        // query for old variant table structure
        //  "SELECT chromosome,start_pos FROM "+variantTable+
        //  "  WHERE sample_id IN(SELECT sample_id FROM sample WHERE patient_id IN(SELECT patient_id FROM patient WHERE map_key=?)) "+
        //  "MINUS "+
        //  "SELECT chromosome,pos FROM GENE_LOCI WHERE map_key=?");

        PreparedStatement ps = conn.prepareStatement(
            "SELECT chromosome,start_pos FROM variant_map_data WHERE map_key=? "+
            "MINUS "+
            "SELECT chromosome,pos FROM GENE_LOCI WHERE map_key=?");

        ps.setInt(1, mapKey);
        ps.setInt(2, mapKey);
        ResultSet rs = ps.executeQuery();

        int cnt = 0;
        while ( rs.next() ) {
            su.update(mapKey, rs.getString(1), rs.getInt(2));
            cnt++;
        }
        conn.close();

        dao.executeBatch(su);

        log.info("  done initializing loci for variants: "+cnt+", elapsed "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    void initLociForDbSnpVariants(int mapKey, String dbSnpBuild) throws Exception {

        log.info("initializing loci for db_snp variants for map_key="+mapKey+" "+dbSnpBuild);

        String sql =
        "INSERT INTO gene_loci (map_key,chromosome,pos) "+
        "VALUES(?,?,?) ";

        BatchSqlUpdate su = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(), sql,
                new int[]{Types.INTEGER, Types.VARCHAR, Types.INTEGER});
        su.compile();

        Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "SELECT chromosome,position FROM db_snp WHERE map_key=? AND source=? "+
            "MINUS "+
            "SELECT chromosome,pos FROM GENE_LOCI WHERE map_key=?");
        ps.setInt(1, mapKey);
        ps.setString(2, dbSnpBuild);
        ps.setInt(3, mapKey);

        long rowCount = 0;
        ResultSet rs = ps.executeQuery();
        while ( rs.next() ) {
            su.update(mapKey, rs.getString(1), rs.getInt(2));
            rowCount++;

            if( rowCount%100000==1 )
                log.info("init_loci "+rowCount);
        }
        conn.close();

        dao.executeBatch(su);

        log.info(rowCount+" inserted loci for db_snp variants for map_key="+mapKey+" "+dbSnpBuild);
    }

    void removeDuplicateLoci(int mapKey) throws Exception {
        String sql =
        "DELETE FROM gene_loci WHERE rowid IN("+
        " SELECT rid FROM ( "+
        "  SELECT rid,rank() OVER(PARTITION BY chromosome,pos ORDER BY rid) AS rank"+
        "  FROM ("+
        "    SELECT chromosome,pos,rowid as rid FROM gene_loci l"+
        "    WHERE (map_key,chromosome,pos) IN("+
        "       SELECT ? map_key,chromosome,pos"+
        "       FROM GENE_LOCI l"+
        "       WHERE MAP_KEY=?"+
        "       GROUP BY CHROMOSOME,POS"+
        "       HAVING COUNT(*)>1 )"+
        "  ) x"+
        " ) WHERE rank>1)";

        Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, mapKey);
        ps.setInt(2, mapKey);
        int rowCount = ps.executeUpdate();
        log.info("removeDuplicateLoci for map_key=" + mapKey+" rows:"+rowCount);
        conn.close();
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
        public boolean insertFlag;

        @Override
        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }
    }
}

