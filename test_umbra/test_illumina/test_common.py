"""
Helpers for other test modules.
"""

def make_bcl_stats_dict(**kwargs):
    """Set up a default BCL stats dictionary with zeros."""
    data = {
        'cycle': 0,
        'avg_intensity': 0.0,
        'avg_int_all_A': 0.0,
        'avg_int_all_C': 0.0,
        'avg_int_all_G': 0.0,
        'avg_int_all_T': 0.0,
        'avg_int_cluster_A': 0.0,
        'avg_int_cluster_C': 0.0,
        'avg_int_cluster_G': 0.0,
        'avg_int_cluster_T': 0.0,
        'num_clust_call_A': 0,
        'num_clust_call_C': 0,
        'num_clust_call_G': 0,
        'num_clust_call_T': 0,
        'num_clust_call_X': 0,
        'num_clust_int_A': 0,
        'num_clust_int_C': 0,
        'num_clust_int_G': 0,
        'num_clust_int_T': 0}
    data.update(kwargs)
    return data


def dummy_bcl_stats(cycles, lanes):
    """Build mock bcl stats list with zeros."""
    expected = []
    for lane in range(1, lanes+1):
        for cycle in range(cycles):
            for tile in [1101, 1102]:
                bcl = make_bcl_stats_dict(cycle=cycle, lane=lane, tile=tile)
                expected.append(bcl)
    return expected
