"""
This script takes fauna fasta files and produces filtered and subsetted JSONs
To be processed by augur

* RUN THIS SCRIPT IN THE H7N9 DIRECTORY!!!!

"""
from __future__ import print_function
import os, sys
sys.path.append('..') # we assume (and assert) that this script is running from the virus directory, i.e. inside H7N9 or zika
from base.prepare import prepare
from datetime import datetime
from base.utils import fix_names

dropped_strains = [
    "A/Chicken/Netherlands/16007311-037041/2016", # not part of epi clade
    "A/Chicken/Netherlands/1600", # not part of epi clade
    "A/duck/Zhejiang/LS02/2014", # not of part of epi clade
    "A/British Columbia/1/2015", # travel case. throws off map
]

config = {
    "dir": "H7N9", # the current directory. You mush be inside this to run the script.
    "file_prefix": "flu_H7N9",
    # "segments": ["HA", "NA"],
    "segments": ["PB2", "PB1", "PA", "HA", "NP", "NA", "MP", "NS"], # set to False, or ["something"] if not segmented...
    "input_format": "fasta",
    "input_paths": [
        "../../fauna/data/h7n9_pb2.fasta",
        "../../fauna/data/h7n9_pb1.fasta",
        "../../fauna/data/h7n9_pa.fasta",
        "../../fauna/data/h7n9_ha.fasta",
        "../../fauna/data/h7n9_np.fasta",
        "../../fauna/data/h7n9_na.fasta",
        "../../fauna/data/h7n9_mp.fasta",
        "../../fauna/data/h7n9_ns.fasta",
        # "test_input/h7n9.ha.fasta",
        # "test_input/h7n9.na.fasta"
    ],
    "output_folder": "prepared",
    # note that "strain" is essential and "date" is special
    "header_fields": {
        0:'strain', 2:'accession', 3:'date', 4:'host', 6:'country', 7: 'division', 11: 'fauna_date'
    },
    # can provide multiple date formats here - FIFO
    "date_format": ["%Y-%m-%d"],
    "require_dates": True,

    # require that all selected isolates have all the genomes
    "ensure_all_segments": True,

    # see docs for help with filters - nested tuples abound
    "filters": (
        ("Dropped Strains", lambda s: s.id not in [fix_names(x) for x in dropped_strains]),
        ("Prior to Epidemic", lambda s: s.attributes['date'] >= datetime(2013,1,1).date()),
        ("Exclude bad host", lambda s: s.attributes["host"] not in ["laboratoryderived", "watersample"]),
        ("Sequence Length", {
            "PB2": lambda s: len(s.seq)>=2200,
            "PB1": lambda s: len(s.seq)>=2200,
            "PA": lambda s: len(s.seq)>=2100,
            "HA": lambda s: len(s.seq)>=1500,
            "NP": lambda s: len(s.seq)>=1400,
            "NA": lambda s: len(s.seq)>=1200,
            "MP": lambda s: len(s.seq)>=900,
            "NS": lambda s: len(s.seq)>=800
        })
    ),
    # see the docs for this too! if you don't want to subsample, set it to False
    # "subsample": False,
    "subsample": {
        "category": None,
        "priority": None,
        "threshold": 1,
    },

    # see the docs for what's going on with colours (sic) & lat/longs
    "colors": ["country", "division", "host"], # essential. Maybe False.
    "lat_longs": ["country", "division"], # essential. Maybe False.
    "lat_long_defs": '../../fauna/source-data/geo_lat_long.tsv',

    # again, see docs for reference definitions
    "references": {
        "PB2": {
            "path": "reference_segments/pb2.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026422", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["PB2"]
        },
        "PB1": {
            "path": "reference_segments/pb1.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026423", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["PB1", "PB1-F2"]
        },
        "PA": {
            "path": "reference_segments/pa.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026424", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["PA", "PA-X"]
        },
        "HA": {
            "path": "reference_segments/ha.gb",
            "metadata": {
                'strain': "reference", "accession": "KJ411975", "date": "2014-01-03",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["SigPep", "HA1", "HA2"]
        },
        "NP": {
            "path": "reference_segments/np.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026426", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["NP"]
        },
        "NA": {
            "path": "reference_segments/na.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026429", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["NA"]
        },
        "NP": {
            "path": "reference_segments/np.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026426", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["NP"]
        },
        "NS": {
            "path": "reference_segments/ns.gb",
            "metadata": {
                'strain': "reference", "accession": "NC_026428", "date": "2013-03-05",
                'host': "human", 'country': "china", 'division': "Shanghai"
            },
            "use": True,
            "genes": ["NEP", "NS1"]
        },
    }

}


if __name__=="__main__":
    runner = prepare(config)
    runner.load_references()
    runner.applyFilters()
    runner.ensure_all_segments()
    runner.subsample()
    runner.colors()
    runner.latlongs()
    runner.write_to_json()
