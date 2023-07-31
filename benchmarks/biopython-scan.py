# Use biopython to count the number of records all the files in the data directory

from Bio.SeqIO import parse
from pathlib import Path

# DATA = (Path.home() / "data" / "uniref10.fasta.split").glob("*.fasta")
DATA = [Path.home() / "data" / "uniref10.fasta.split" / "uniref10.part_001.fasta"]

total_records = 0

for file in DATA:
    with open(file, "r") as handle:
        for record in parse(handle, "fasta"):
            total_records += 1

print(total_records)
