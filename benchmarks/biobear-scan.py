# Use biobear to count the number of records all the files in the data directory

from pathlib import Path
from biobear import FastaReader

# DATA = Path.home() / "data" / "uniref10.fasta.split"
DATA = Path.home() / "data" / "uniref10.fasta.split" / "uniref10.part_001.fasta"

total_records = 0

for batch in FastaReader(DATA).to_arrow():
    total_records += len(batch)

print(total_records)
