package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.DataSourceFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: mtutaj
 * Date: 2/6/13
 * Time: 11:12 AM
 * <p>
 * a module to print out nonsynonymous exonic snps for specified samples
 */
public class SnpInGeneCount {

    public static void main(String[] args) throws Exception {

        SnpInGeneCount instance = new SnpInGeneCount();
        instance.run();
    }

    public void run() throws Exception {

        System.out.println("SnpInGeneCount module v. 1.0 starting");

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("nonsynonymous_exonic_snps.txt")));
        writer.println("#generated on "+new Date());
        writer.print("#gene_symbol\tgene_rgd_id\tchromosome\tnucl_acc_id\tprotein_acc_id");

        // aggregate data for all samples
        // key: transcript_acc_id
        Map<String, Record> data = new HashMap<String, Record>();

        for(int sampleId=500; sampleId<=523; sampleId++) {
            String sampleName = getSampleName(sampleId);
            writer.write("\t"+sampleName);

            processSample(sampleId, data);
        }
        writer.println();

        dumpData(writer, data);
        System.out.println("DONE");
    }

    Connection getConnection() throws Exception {

        Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection();
        return conn;
    }

    String getSampleName(int sampleId) throws Exception {

        String sampleName = "";
        Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement("select analysis_name from sample where sample_id=?");
        ps.setInt(1, sampleId);
        ResultSet rs = ps.executeQuery();
        if( rs.next() ) {
            sampleName = rs.getString(1);
        }
        ps.close();
        conn.close();

        return sampleName;
    }

    void processSample(int sampleId, Map<String, Record> data) throws Exception {

        System.out.println("processing sample "+sampleId);


        // get variant count
        Connection conn = getConnection();
        PreparedStatement ps = conn.prepareStatement(
            "select chromosome,acc_id,protein_acc_id,gene_rgd_id,gene_symbol,count(*) variant_count\n" +
            "from variant v,variant_transcript vt,transcripts t,genes g\n" +
            "where sample_id=?  \n" +
            "and v.variant_id=vt.variant_id and vt.transcript_rgd_id=t.transcript_rgd_id and t.gene_rgd_id=g.rgd_id\n" +
            "and syn_status='nonsynonymous' and location_name='EXON'\n" +
            "group by chromosome,acc_id,protein_acc_id,gene_rgd_id,gene_symbol");


        ps.setInt(1, sampleId);
        ResultSet rs = ps.executeQuery();
        while ( rs.next() ) {

            String accId = rs.getString("acc_id");
            Record rec = data.get(accId);
            if( rec==null ) {
                rec = new Record();
                data.put(accId, rec);

                rec.accId = accId;
                rec.chromosome = rs.getString("chromosome");
                rec.geneRgdId = rs.getInt("gene_rgd_id");
                rec.geneSymbol = rs.getString("gene_symbol");
                rec.proteinAccId = rs.getString("protein_acc_id");
                if( rec.proteinAccId==null )
                    rec.proteinAccId = "";
            }
            rec.varCount[sampleId-500] = rs.getInt("variant_count");
        }
        rs.close();
        conn.close();
    }

    void dumpData(PrintWriter writer, Map<String, Record> data) {

        System.out.println("sorting data");

        List<Record> dataList = new ArrayList<Record>(data.values());
        Collections.sort(dataList, new Comparator<Record>() {
            public int compare(Record o1, Record o2) {
                int r = o1.geneSymbol.compareToIgnoreCase(o2.geneSymbol);
                if( r!=0 )
                    return r;
                return o1.accId.compareTo(o2.accId);
            }
        });


        System.out.println("writing data");

        for( Record rec: dataList ) {


            writer.print(rec.geneSymbol);
            writer.print('\t');
            writer.print(rec.geneRgdId);
            writer.print('\t');
            writer.print(rec.chromosome);
            writer.print('\t');
            writer.print(rec.accId);
            writer.print('\t');
            writer.print(rec.proteinAccId);
            writer.print('\t');
            for( int varCount: rec.varCount ) {
                if( varCount!=0 )
                    writer.print(varCount);
                writer.print('\t');
            }
            writer.println();
        }
        writer.close();
    }

    class Record {
        int geneRgdId;
        String geneSymbol;
        String chromosome;
        String accId;
        String proteinAccId;
        // variant count for samples 500..523
        int[] varCount = new int[24];
    }
}
