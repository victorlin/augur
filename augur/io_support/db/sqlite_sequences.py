import sqlite3
import zlib
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord

from augur.io import open_file

SEQUENCE_ID_COLUMN = 'id'
SEQUENCE_VALUE_COLUMN = 'seq'

def load_fasta(fasta_file:str, connection:sqlite3.Connection, table_name:str):
    """Loads sequence data from a FASTA file."""
    with connection:
        create_table_statement = f"""
            CREATE TABLE {table_name} (
                {SEQUENCE_ID_COLUMN} TEXT,
                {SEQUENCE_VALUE_COLUMN} BLOB
            )
        """
        connection.execute(create_table_statement)

    insert_statement = f"""
        INSERT INTO {table_name}
        VALUES (?,?)
    """
    # TODO: format=VCF
    rows = _iter_sequences(fasta_file)
    try:
        with connection:
            connection.executemany(insert_statement, rows)
    except sqlite3.ProgrammingError as e:
        raise ValueError(f'Failed to load {fasta_file}.') from e


def _iter_sequences(fasta_file:str, format="fasta"):
    """Yield sequences."""
    with open_file(fasta_file) as f:
        records = SeqIO.parse(f, format)
        for record in records:
            # yield (record.id, str(record.seq))
            yield (record.id, zlib.compress(str(record.seq).encode()))

def write_fasta(fasta_file:str, connection:sqlite3.Connection, table_name:str):
    rows = connection.execute(f"""
        SELECT {SEQUENCE_ID_COLUMN}, {SEQUENCE_VALUE_COLUMN}
        FROM {table_name}
    """)
    with open_file(fasta_file, 'w') as f:
        for row in rows:
            record = SeqRecord(
                Seq(zlib.decompress(row[1]).decode('UTF-8')),
                id=row[0],
                description=''
            )
            SeqIO.write(record, f, "fasta-2line")
