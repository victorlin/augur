import pytest
from augur.filter_support.exceptions import FilterException
from augur.filter_support.db.sqlite import (
    EXCLUDE_COL,
    FILTER_REASON_COL,
    SUBSAMPLE_FILTER_REASON,
    METADATA_FILTER_REASON_TABLE_NAME,
)
from test_filter import (
    get_filter_obj_run,
    get_valid_args,
    query_fetchall,
)


class TestSubsampling:
    def test_subsample_max_sequences(self, tmpdir):
        """Randomly sample 6 out of 10 sequences, removing 4 due to subsampling."""
        data = [
            ('strain',),
            ('SEQ1',),
            ('SEQ2',),
            ('SEQ3',),
            ('SEQ4',),
            ('SEQ5',),
            ('SEQ6',),
            ('SEQ7',),
            ('SEQ8',),
            ('SEQ9',),
            ('SEQ10',)
        ]
        args = get_valid_args(data, tmpdir)
        args.subsample_seed = 1234
        args.subsample_max_sequences = 6
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
        """)
        assert results == [('SEQ3',), ('SEQ5',), ('SEQ6',), ('SEQ9',)]

    def test_subsample_max_sequences_with_min_date(self, tmpdir):
        """Randomly sample 6 out of 10 sequences.

        - Remove 1 due to min_date
        - Remove 3 due to subsampling
        """
        data = [
            ('strain','date'),
            ('SEQ1','2018'),
            ('SEQ2','2019'),
            ('SEQ3','2020'),
            ('SEQ4','2020'),
            ('SEQ5','2020'),
            ('SEQ6','2020'),
            ('SEQ7','2020'),
            ('SEQ8','2020'),
            ('SEQ9','2020'),
            ('SEQ10','2020')
        ]
        args = get_valid_args(data, tmpdir)
        args.subsample_seed = 1234
        args.min_date = '2019'
        args.subsample_max_sequences = 6
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT {FILTER_REASON_COL}, strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {EXCLUDE_COL}
        """)
        assert results == [
            ('filter_by_min_date', 'SEQ1'),
            ('subsampling', 'SEQ4'),
            ('subsampling', 'SEQ6'),
            ('subsampling', 'SEQ7')
        ]

    def test_no_probabilistic_sampling_error(self, tmpdir):
        """Requesting less sequences than number of groups without allowing probabilistic sampling raises an error."""
        data = [
            ('strain','group'),
            ('SEQ1','A'),
            ('SEQ2','B'),
            ('SEQ3','C'),
            ('SEQ4','D'),
            ('SEQ5','E')
        ]
        args = get_valid_args(data, tmpdir)
        args.subsample_seed = 1234
        args.group_by = ['group']
        args.subsample_max_sequences = 3
        args.probabilistic_sampling = False
        with pytest.raises(FilterException) as e_info:
            get_filter_obj_run(args)
        assert str(e_info.value) == 'Asked to provide at most 3 sequences, but there are 5 groups.'

    def test_probabilistic_sampling_warn(self, tmpdir, capsys):
        """Requesting less sequences than number of groups results in probabilistic sampling as default behavior."""
        data = [
            ('strain','group'),
            ('SEQ1','A'),
            ('SEQ2','B'),
            ('SEQ3','C'),
            ('SEQ4','D'),
            ('SEQ5','E')
        ]
        args = get_valid_args(data, tmpdir)
        args.subsample_seed = 4321
        args.group_by = ['group']
        args.subsample_max_sequences = 3
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
        """)
        assert results == [('SEQ1',), ('SEQ3',), ('SEQ4',)]
        captured = capsys.readouterr()
        assert captured.err == "WARNING: Asked to provide at most 3 sequences, but there are 5 groups.\n"
        assert captured.out.startswith("Sampling probabilistically at 0.5859 sequences per group, meaning it is possible to have more than the requested maximum of 3 sequences after filtering.")

    def test_sequences_per_group(self, tmpdir):
        """Subsample with a maximum of 2 sequences per group."""
        data = [
            ('strain','group'),
            ('SEQ1','A'),
            ('SEQ2','A'),
            ('SEQ3','A'),
            ('SEQ4','B'),
            ('SEQ5','C')
        ]
        args = get_valid_args(data, tmpdir)
        args.subsample_seed = 1234
        args.group_by = ['group']
        args.sequences_per_group = 2
        filter_obj = get_filter_obj_run(args)
        results = query_fetchall(filter_obj, f"""
            SELECT strain
            FROM {METADATA_FILTER_REASON_TABLE_NAME}
            WHERE {FILTER_REASON_COL} = '{SUBSAMPLE_FILTER_REASON}'
        """)
        assert results == [('SEQ2',)]
