package edu.mcw.rgd.pipelines;

/**
 * @author mtutaj
 * since 11/18/13
 * information to compute gene loci information for a species and assembly
 */
public class RunInfo {

    private int mapKey;
    private String dbSnpBuild;
    private int speciesTypeKey;
    private boolean runIt;


    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public String getDbSnpBuild() {
        return dbSnpBuild;
    }

    public void setDbSnpBuild(String dbSnpBuild) {
        this.dbSnpBuild = dbSnpBuild;
    }

    public int getSpeciesTypeKey() {
        return speciesTypeKey;
    }

    public void setSpeciesTypeKey(int speciesTypeKey) {
        this.speciesTypeKey = speciesTypeKey;
    }

    public boolean isRunIt() {
        return runIt;
    }

    public void setRunIt(boolean runIt) {
        this.runIt = runIt;
    }
}
