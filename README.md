# gene-loci-pipeline

Populates the GENE_LOCI table used by the Variant Visualizer tool. For each genome assembly, the pipeline scans every chromosome position where variants exist and records which genes overlap that position, building a lookup table of genic status and gene symbols by position.

## Algorithm

For each configured species/assembly and chromosome:

1. **Load** all active genes and their genomic positions from the database.
2. **Sweep** through variant positions on the chromosome, tracking which genes are "in frame" at each position.
3. **Classify** each variant position as genic (within one or more genes) or intergenic (between genes), recording associated gene symbols.
4. **QC** new loci against existing GENE_LOCI rows: insert new rows, skip unchanged rows, and delete obsolete rows.

## Supported Species and Assemblies

| Species       | Assemblies                                           |
|---------------|------------------------------------------------------|
| Rat           | GRCr8, Rnor_6.0, RGSC_v3.4, Rnor_5.0, mRatBN7.2   |
| Mouse         | GRCm37, GRCm38, GRCm39                              |
| Human         | GRCh37, GRCh38                                       |
| Dog           | CanFam3.1, ROS_Cfam_1.0                              |
| Pig           | Sscrofa10.2, Sscrofa11.1                             |
| Green Monkey  | ChlSab1.1, Vero_WHO_p1.0                             |

## Usage

**Standard mode** -- process all enabled assemblies:
```
java -jar gene-loci-pipeline.jar
```

**Single assembly:**
```
java -jar gene-loci-pipeline.jar --mapKey=380
```

**Clean duplicates** for a specific or all assemblies:
```
java -jar gene-loci-pipeline.jar --cleanDuplicates --mapKey=380
java -jar gene-loci-pipeline.jar --cleanDuplicates --mapKey=all
```

## Output

- `gene_loci` table rows with: map_key, chromosome, position, genic_status, gene_symbols
- Audit logs: `logs/inserted.log`, `logs/deleted.log`, `logs/duplicates.log`
- Pipeline status: `logs/core.log`, `logs/summary.log`
