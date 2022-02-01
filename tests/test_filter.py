import argparse
import random
import shlex

import pytest

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord

import augur.filter
from augur.utils import read_metadata

@pytest.fixture
def argparser():
    parser = argparse.ArgumentParser()
    augur.filter.register_arguments(parser)
    def parse(args):
        return parser.parse_args(shlex.split(args))
    return parse

@pytest.fixture
def sequences():
    def random_seq(k):
        return "".join(random.choices(("A","T","G","C"), k=k))
    return {
        "SEQ_1": SeqRecord(Seq(random_seq(10)), id="SEQ_1"),
        "SEQ_2": SeqRecord(Seq(random_seq(10)), id="SEQ_2"),
        "SEQ_3": SeqRecord(Seq(random_seq(10)), id="SEQ_3"),
    }

@pytest.fixture
def fasta_fn(tmpdir, sequences):
    fn = str(tmpdir / "sequences.fasta")
    SeqIO.write(sequences.values(), fn, "fasta")
    return fn

def write_metadata(tmpdir, metadata):
    fn = str(tmpdir / "metadata.tsv")
    with open(fn, "w") as fh:
        fh.write("\n".join(("\t".join(md) for md in metadata)))
    return fn


class TestFilter:
    def test_filter_run_min_date(self, tmpdir, fasta_fn, argparser):
        """Test that filter --min-date is inclusive"""
        out_fn = str(tmpdir / "out.fasta")
        min_date = "2020-02-26"
        meta_fn = write_metadata(tmpdir, (("strain","date"),
                                          ("SEQ_1","2020-02-XX"),
                                          ("SEQ_2","2020-02-26"),
                                          ("SEQ_3","2020-02-25")))
        args = argparser('-s %s --metadata %s -o %s --min-date %s'
                         % (fasta_fn, meta_fn, out_fn, min_date))
        augur.filter.run(args)
        output = SeqIO.to_dict(SeqIO.parse(out_fn, "fasta"))
        assert list(output.keys()) == ["SEQ_1", "SEQ_2"]

    def test_filter_run_max_date(self, tmpdir, fasta_fn, argparser):
        """Test that filter --max-date is inclusive"""
        out_fn = str(tmpdir / "out.fasta")
        max_date = "2020-03-01"
        meta_fn = write_metadata(tmpdir, (("strain","date"),
                                          ("SEQ_1","2020-03-XX"),
                                          ("SEQ_2","2020-03-01"),
                                          ("SEQ_3","2020-03-02")))
        args = argparser('-s %s --metadata %s -o %s --max-date %s'
                         % (fasta_fn, meta_fn, out_fn, max_date))
        augur.filter.run(args)
        output = SeqIO.to_dict(SeqIO.parse(out_fn, "fasta"))
        assert list(output.keys()) == ["SEQ_1", "SEQ_2"]
