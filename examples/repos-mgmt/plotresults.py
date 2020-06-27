# -*- coding: utf-8 -*-
"""Plot results read from a result set
"""
from __future__ import division
import os
import argparse
import collections
import logging

import numpy as np
import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt

from icarus.util import Settings, Tree, config_logging, step_cdf
from icarus.tools import means_confidence_interval
from icarus.results import plot_lines, plot_bar_chart
from icarus.registry import RESULTS_READER

# Logger object
logger = logging.getLogger('plot')

# These lines prevent insertion of Type 3 fonts in figures
# Publishers don't want them
plt.rcParams['ps.useafm'] = True
plt.rcParams['pdf.use14corefonts'] = True

# If True text is interpreted as LaTeX, e.g. underscore are interpreted as 
# subscript. If False, text is interpreted literally
plt.rcParams['text.usetex'] = False

# Aspect ratio of the output figures
plt.rcParams['figure.figsize'] = 8, 5

# Size of font in legends
LEGEND_SIZE = 14

# Line width in pixels
LINE_WIDTH = 1.5

# Plot
PLOT_EMPTY_GRAPHS = True

# This dict maps strategy names to the style of the line to be used in the plots
# Off-path strategies: solid lines
# On-path strategies: dashed lines
# No-cache: dotted line
STRATEGY_STYLE = {
    'HR_SYMM': 'b-o',
    'HR_ASYMM': 'g-D',
    'HR_MULTICAST': 'm-^',
    'HR_HYBRID_AM': 'c-s',
    'HR_HYBRID_SM': 'r-v',
    'LCE': 'b--p',
    'LCD': 'g-->',
    'CL4M': 'g-->',
    'PROB_CACHE': 'c--<',
    'RAND_CHOICE': 'r--<',
    'RAND_BERNOULLI': 'g--*',
    'NO_CACHE': 'k:o',
    'OPTIMAL': 'k-o',
    'HYBRIDS_REPO_APP': 'b:x',
    'HYBRIDS_PRO_REPO_APP': 'b-x',
    'HYBRIDS_RE_REPO_APP': 'b--x',
    'HYBRIDS_SPEC_REPO_APP': 'b.x'
}

# This dict maps name of strategies to names to be displayed in the legend
STRATEGY_LEGEND = {
    'LCE': 'LCE',
    'LCD': 'LCD',
    'HR_SYMM': 'HR Symm',
    'HR_ASYMM': 'HR Asymm',
    'HR_MULTICAST': 'HR Multicast',
    'HR_HYBRID_AM': 'HR Hybrid AM',
    'HR_HYBRID_SM': 'HR Hybrid SM',
    'CL4M': 'CL4M',
    'PROB_CACHE': 'ProbCache',
    'RAND_CHOICE': 'Random (choice)',
    'RAND_BERNOULLI': 'Random (Bernoulli)',
    'NO_CACHE': 'No caching',
    'OPTIMAL': 'Optimal',
    'HYBRIDS_REPO_APP': 'b:x',
    'HYBRIDS_PRO_REPO_APP': 'b-x',
    'HYBRIDS_RE_REPO_APP': 'b--x',
    'HYBRIDS_SPEC_REPO_APP': 'b.x'
}

# Color and hatch styles for bar charts of cache hit ratio and link load vs topology
STRATEGY_BAR_COLOR = {
    'LCE': 'k',
    'LCD': '0.4',
    'NO_CACHE': '0.5',
    'HR_ASYMM': '0.6',
    'HR_SYMM': '0.7',
    'HYBRIDS_REPO_APP': '0.8',
    'HYBRIDS_PRO_REPO_APP': '0.9',
    'HYBRIDS_RE_REPO_APP': '0.65',
    'HYBRIDS_SPEC_REPO_APP': '0.75'
}

STRATEGY_BAR_HATCH = {
    'LCE': None,
    'LCD': '//',
    'NO_CACHE': 'x',
    'HR_ASYMM': '+',
    'HR_SYMM': '\\',
    'HYBRIDS_REPO_APP': '||',
    'HYBRIDS_PRO_REPO_APP': '*',
    'HYBRIDS_RE_REPO_APP': '**',
    'HYBRIDS_SPEC_REPO_APP': '|'
}


def plot_cache_hits_vs_alpha(resultset, topology, cache_size, alpha_range, strategies, plotdir):
    if 'NO_CACHE' in strategies:
        strategies.remove('NO_CACHE')
    desc = {}
    desc['title'] = 'Cache hit ratio: T=%s C=%s' % (topology, cache_size)
    desc['ylabel'] = 'Cache hit ratio'
    desc['xlabel'] = u'Content distribution \u03b1'
    desc['xparam'] = ('workload', 'alpha')
    desc['xvals'] = alpha_range
    desc['filter'] = {'topology': {'name': topology},
                      'cache_placement': {'network_cache': cache_size}}
    desc['ymetrics'] = [('CACHE_HIT_RATIO', 'MEAN')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper left'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'CACHE_HIT_RATIO_T=%s@C=%s.pdf'
               % (topology, cache_size), plotdir)


def plot_cache_hits_vs_cache_size(resultset, topology, alpha, cache_size_range, strategies, plotdir):
    desc = {}
    if 'NO_CACHE' in strategies:
        strategies.remove('NO_CACHE')
    desc['title'] = 'Cache hit ratio: T=%s A=%s' % (topology, alpha)
    desc['xlabel'] = u'Cache to population ratio'
    desc['ylabel'] = 'Cache hit ratio'
    desc['xscale'] = 'log'
    desc['xparam'] = ('cache_placement', 'network_cache')
    desc['xvals'] = cache_size_range
    desc['filter'] = {'topology': {'name': topology},
                      'workload': {'name': 'STATIONARY', 'alpha': alpha}}
    desc['ymetrics'] = [('CACHE_HIT_RATIO', 'MEAN')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper left'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'CACHE_HIT_RATIO_T=%s@A=%s.pdf'
               % (topology, alpha), plotdir)


def plot_link_load_vs_alpha(resultset, topology, cache_size, alpha_range, strategies, plotdir):
    desc = {}
    desc['title'] = 'Internal link load: T=%s C=%s' % (topology, cache_size)
    desc['xlabel'] = u'Content distribution \u03b1'
    desc['ylabel'] = 'Internal link load'
    desc['xparam'] = ('workload', 'alpha')
    desc['xvals'] = alpha_range
    desc['filter'] = {'topology': {'name': topology},
                      'cache_placement': {'network_cache': cache_size}}
    desc['ymetrics'] = [('LINK_LOAD', 'MEAN_INTERNAL')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper right'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'LINK_LOAD_INTERNAL_T=%s@C=%s.pdf'
               % (topology, cache_size), plotdir)


def plot_link_load_vs_cache_size(resultset, topology, alpha, cache_size_range, strategies, plotdir):
    desc = {}
    desc['title'] = 'Internal link load: T=%s A=%s' % (topology, alpha)
    desc['xlabel'] = 'Cache to population ratio'
    desc['ylabel'] = 'Internal link load'
    desc['xscale'] = 'log'
    desc['xparam'] = ('cache_placement', 'network_cache')
    desc['xvals'] = cache_size_range
    desc['filter'] = {'topology': {'name': topology},
                      'workload': {'name': 'stationary', 'alpha': alpha}}
    desc['ymetrics'] = [('LINK_LOAD', 'MEAN_INTERNAL')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper right'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'LINK_LOAD_INTERNAL_T=%s@A=%s.pdf'
               % (topology, alpha), plotdir)


def plot_latency_vs_alpha(resultset, topology, cache_size, alpha_range, strategies, plotdir):
    desc = {}
    desc['title'] = 'Latency: T=%s C=%s' % (topology, cache_size)
    desc['xlabel'] = u'Content distribution \u03b1'
    desc['ylabel'] = 'Latency (ms)'
    desc['xparam'] = ('workload', 'alpha')
    desc['xvals'] = alpha_range
    desc['filter'] = {'topology': {'name': topology},
                      'cache_placement': {'network_cache': cache_size}}
    desc['ymetrics'] = [('LATENCY', 'MEAN')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper right'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'LATENCY_T=%s@C=%s.pdf'
               % (topology, cache_size), plotdir)


def plot_latency_vs_cache_size(resultset, topology, alpha, cache_size_range, strategies, plotdir):
    desc = {}
    desc['title'] = 'Latency: T=%s A=%s' % (topology, alpha)
    desc['xlabel'] = 'Cache to population ratio'
    desc['ylabel'] = 'Latency'
    desc['xscale'] = 'log'
    desc['xparam'] = ('cache_placement', 'network_cache')
    desc['xvals'] = cache_size_range
    desc['filter'] = {'topology': {'name': topology},
                      'workload': {'name': 'STATIONARY', 'alpha': alpha}}
    desc['ymetrics'] = [('LATENCY', 'MEAN')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['metric'] = ('LATENCY', 'MEAN')
    desc['errorbar'] = True
    desc['legend_loc'] = 'upper right'
    desc['line_style'] = STRATEGY_STYLE
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_lines(resultset, desc, 'LATENCY_T=%s@A=%s.pdf'
               % (topology, alpha), plotdir)


def plot_cache_hits_vs_topology(resultset, alpha, cache_size, topology_range, strategies, plotdir):
    """
    Plot bar graphs of cache hit ratio for specific values of alpha and cache
    size for various topologies.

    The objective here is to show that our algorithms works well on all
    topologies considered
    """
    if 'NO_CACHE' in strategies:
        strategies.remove('NO_CACHE')
    desc = {}
    desc['title'] = 'Cache hit ratio: A=%s C=%s' % (alpha, cache_size)
    desc['ylabel'] = 'Cache hit ratio'
    desc['xparam'] = ('topology', 'name')
    desc['xvals'] = topology_range
    desc['filter'] = {'cache_placement': {'network_cache': cache_size},
                      'workload': {'name': 'STATIONARY', 'alpha': alpha}}
    desc['ymetrics'] = [('CACHE_HIT_RATIO', 'MEAN')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'lower right'
    desc['bar_color'] = STRATEGY_BAR_COLOR
    desc['bar_hatch'] = STRATEGY_BAR_HATCH
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_bar_chart(resultset, desc, 'CACHE_HIT_RATIO_A=%s_C=%s.pdf'
                   % (alpha, cache_size), plotdir)


def plot_link_load_vs_topology(resultset, alpha, cache_size, topology_range, strategies, plotdir):
    """
    Plot bar graphs of link load for specific values of alpha and cache
    size for various topologies.

    The objective here is to show that our algorithms works well on all
    topologies considered
    """
    desc = {}
    desc['title'] = 'Internal link load: A=%s C=%s' % (alpha, cache_size)
    desc['ylabel'] = 'Internal link load'
    desc['xparam'] = ('topology', 'name')
    desc['xvals'] = topology_range
    desc['filter'] = {'cache_placement': {'network_cache': cache_size},
                      'workload': {'name': 'STATIONARY', 'alpha': alpha}}
    desc['ymetrics'] = [('LINK_LOAD', 'MEAN_INTERNAL')] * len(strategies)
    desc['ycondnames'] = [('strategy', 'name')] * len(strategies)
    desc['ycondvals'] = strategies
    desc['errorbar'] = True
    desc['legend_loc'] = 'lower right'
    desc['bar_color'] = STRATEGY_BAR_COLOR
    desc['bar_hatch'] = STRATEGY_BAR_HATCH
    desc['legend'] = STRATEGY_LEGEND
    desc['plotempty'] = PLOT_EMPTY_GRAPHS
    plot_bar_chart(resultset, desc, 'LINK_LOAD_INTERNAL_A=%s_C=%s.pdf'
                   % (alpha, cache_size), plotdir)


def searchDictMultipleCat(lst, category_list, attr_value_pairs, num_pairs, collector, subtype):
    """
    Search the resultset list for a particular [category, attribute, value] parameter such as ['strategy', 'extra_quota', 3]. attr_value_pairs include the key-value pairs.
    and once such a key is found, extract the result for a collector, subtype such as ['CACHE_HIT_RATIO', 'MEAN']

    Returns the result if found in the dictionary lst; otherwise returns None

    """
    result = None
    for l in lst:
        num_match = 0
        for key, val in l[0].items():
            # print key + '-and-' + category + '-\n'
            if key in category_list:
                if (isinstance(val, dict)):
                    for key1, val1 in val.items():
                        for key2, val2 in attr_value_pairs.items():
                            if key1 == key2 and val1 == val2:
                                num_match = num_match + 1
                    if num_match == num_pairs:
                        result = l[1]
                        break
                else:
                    print 'Something is wrong with the search for attr-value pairs\n'
                    return None

        if result is not None:
            break

    if result is None:
        print 'Error searched attribute, value pairs:\n'
        for k, v in attr_value_pairs.items():
            print '[ ' + repr(k) + ' , ' + repr(v) + ' ]  '
        print 'is not found, returning none\n'
        return None

    found = None
    for key, val in result.items():
        if key == collector:
            for key1, val1 in val.items():
                if key1 == subtype:
                    found = val1
                    break
            if found is not None:
                break

    if found is None:
        print 'Error searched collector, subtype ' + repr(collector) + ',' + repr(subtype) + 'is not found\n'

    return found


def searchDictMultipleCat1(lst, category_list, attr_value_list, num_pairs, collector, subtype):
    """
    Search the resultset list for a particular [category, attribute, value] parameter such as ['strategy', 'extra_quota', 3]. attr_value_pairs include the key-value pairs.
    and once such a key is found, extract the result for a collector, subtype such as ['CACHE_HIT_RATIO', 'MEAN']

    Returns the result if found in the dictionary lst; otherwise returns None

    """
    result = None
    for l in lst:
        num_match = 0
        for key, val in l[0].items():
            # print key + '-and-' + category + '-\n'
            if key in category_list:
                if (isinstance(val, dict)):
                    for key1, val1 in val.items():
                        for arr in attr_value_list:
                            key2 = arr[0]
                            val2 = arr[1]
                            if key1 == key2 and val1 == val2:
                                num_match = num_match + 1
                    if num_match == num_pairs:
                        result = l[1]
                        break
                else:
                    print 'Something is wrong with the search for attr-value pairs\n'
                    return None

        if result is not None:
            break

    if result is None:
        print 'Error searched attribute, value pairs:\n'
        for arr in attr_value_list:
            k = arr[0]
            v = arr[1]
            print '[ ' + repr(k) + ' , ' + repr(v) + ' ]  '
        print 'is not found, returning none\n'
        return None

    found = None
    for key, val in result.items():
        if key == collector:
            for key1, val1 in val.items():
                if key1 == subtype:
                    found = val1
                    break
            if found is not None:
                break

    if found is None:
        print 'Error searched collector, subtype ' + repr(collector) + ',' + repr(subtype) + 'is not found\n'

    return found


def searchDict(lst, category, attr_value_pairs, num_pairs, collector, subtype):
    """
    Search the resultset list for a particular [category, attribute, value] parameter such as ['strategy', 'extra_quota', 3]. attr_value_pairs include the key-value pairs.
    and once such a key is found, extract the result for a collector, subtype such as ['CACHE_HIT_RATIO', 'MEAN']

    Returns the result if found in the dictionary lst; otherwise returns None

    """
    result = None
    for l in lst:
        for key, val in l[0].items():
            # print key + '-and-' + category + '-\n'
            if key == category:
                if (isinstance(val, dict)):
                    num_match = 0
                    for key1, val1 in val.items():
                        for key2, val2 in attr_value_pairs.items():
                            if key1 == key2 and val1 == val2:
                                num_match = num_match + 1
                    if num_match == num_pairs:
                        result = l[1]
                        break
                else:
                    print 'Something is wrong with the search for attr-value pairs\n'
                    return None
        if result is not None:
            break

    if result is None:
        print 'Error searched attribute, value pairs:\n'
        for k, v in attr_value_pairs.items():
            print '[ ' + repr(k) + ' , ' + repr(v) + ' ]  '
        print 'is not found, returning none\n'
        return None

    found = None
    for key, val in result.items():
        if key == collector:
            for key1, val1 in val.items():
                if key1 == subtype:
                    found = val1
                    break
            if found is not None:
                break

    if found is None:
        print 'Error searched collector, subtype ' + repr(collector) + ',' + repr(subtype) + 'is not found\n'

    return found


def print_lru_probability_results(lst):
    probs = [0.1, 0.25, 0.50, 0.75, 1.0]
    strategies = ['LRU']

    for strategy in strategies:
        for p in probs:
            filename = 'sat_' + str(strategy) + '_' + str(p)
            f = open(filename, 'w')
            f.write('# Sat. rate for LRU over time\n')
            f.write('#\n')
            f.write('# Time     Sat. Rate\n')
            sat_times = searchDict(lst, 'strategy', {'name': strategy, 'p': p}, 2, 'LATENCY', 'SAT_TIMES')
            for k in sorted(sat_times):
                s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
                f.write(s)
            f.close()

    for strategy in strategies:
        for p in probs:
            filename = 'idle_' + str(strategy) + '_' + str(p)
            f = open(filename, 'w')
            f.write('# Idle time of strategies over time\n')
            f.write('#\n')
            f.write('# Time     Idle percentage\n')
            idle_times = searchDict(lst, 'strategy', {'name': strategy, 'p': p}, 2, 'LATENCY', 'IDLE_TIMES')
            for k in sorted(idle_times):
                s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
                f.write(s)
            f.close()


def print_strategies_performance(lst):
    strategies = ['SDF', 'HYBRID', 'MFU', 'COORDINATED']
    # strategies = ['HYBRID']
    service_budget = 500
    alpha = 0.5  # 0.75
    replacement_interval = 30.0
    n_services = 1000

    #  Print Sat. rates:
    for strategy in strategies:
        filename = 'sat_' + str(strategy)
        f = open(filename, 'w')
        f.write('# Sat. rate over time\n')
        f.write('#\n')
        f.write('# Time     Sat. Rate\n')
        sat_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                          {'name': strategy, 'service_budget': service_budget, 'alpha': alpha}, 3,
                                          'LATENCY', 'SAT_TIMES')
        for k in sorted(sat_times):
            s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
            f.write(s)
        f.close()

    #  Print Idle times:
    for strategy in strategies:
        filename = 'idle_' + str(strategy)
        f = open(filename, 'w')
        f.write('# Idle time of strategies over time\n')
        f.write('#\n')
        f.write('# Time     Idle percentage\n')
        idle_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                           {'name': strategy, 'service_budget': service_budget, 'alpha': alpha}, 3,
                                           'LATENCY', 'IDLE_TIMES')
        for k in sorted(idle_times):
            s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
            f.write(s)
        f.close()

    #  Print per-service Sat. rates:
    for strategy in strategies:
        filename = 'sat_service_' + str(strategy)
        f = open(filename, 'w')
        f.write('# Per-service Sat. rate over time\n')
        f.write('#\n')
        f.write('# Time     Sat. Rate\n')
        sat_services = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                             {'name': strategy, 'service_budget': service_budget, 'alpha': alpha}, 3,
                                             'LATENCY', 'PER_SERVICE_SATISFACTION')
        # f.write(str(sat_services))
        for indx in range(1, n_services):
            s = str(indx) + "\t" + str(sat_services[indx]) + "\n"
            f.write(s)
        f.close()


def print_scheduling_experiments(lst):
    strategies = ['SDF', 'HYBRID', 'MFU']
    schedule_policies = ['EDF', 'FIFO']
    service_budget = 500
    alpha = 0.75
    replacement_interval = 30.0

    #  Print Sat. rates:
    for strategy in strategies:
        for policy in schedule_policies:
            filename = 'sat_' + str(strategy) + '_' + str(policy)
            f = open(filename, 'w')
            f.write('# Sat. rate over time\n')
            f.write('#\n')
            f.write('# Time     Sat. Rate\n')
            sat_times = searchDictMultipleCat1(lst, ['strategy', 'computation_placement', 'workload', 'sched_policy'],
                                               [['name', strategy], ['service_budget', service_budget],
                                                ['alpha', alpha], ['name', policy]], 4, 'LATENCY', 'SAT_TIMES')
            for k in sorted(sat_times):
                s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
                f.write(s)
            f.close()

    #  Print idle times:
    for strategy in strategies:
        for policy in schedule_policies:
            filename = 'idle_' + str(strategy) + '_' + str(policy)
            f = open(filename, 'w')
            f.write('# Idle times over time\n')
            f.write('#\n')
            f.write('# Time     Idle percentage\n')
            idle_times = searchDictMultipleCat1(lst, ['strategy', 'computation_placement', 'workload', 'sched_policy'],
                                                [['name', strategy], ['service_budget', service_budget],
                                                 ['alpha', alpha], ['name', policy]], 4, 'LATENCY', 'IDLE_TIMES')
            for k in sorted(idle_times):
                s = str(k[0][0]) + "\t" + str((1.0 * k[1])) + "\n"
                f.write(s)
            f.close()


def print_zipf_experiment(lst):
    strategies = ['SDF', 'HYBRID', 'MFU']
    alphas = [0.1, 0.25, 0.50, 0.75, 1.0]
    replacement_interval = 30.0
    service_budget = 500

    #  Print Sat. rates:
    for strategy in strategies:
        for alpha in alphas:
            filename = 'sat_' + str(strategy) + '_' + str(alpha)
            f = open(filename, 'w')
            f.write('# Sat. rate over time\n')
            f.write('#\n')
            f.write('# Time     Sat. Rate\n')
            sat_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                              {'name': strategy, 'service_budget': service_budget, 'alpha': alpha}, 3,
                                              'LATENCY', 'SAT_TIMES')
            for k in sorted(sat_times):
                s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
                f.write(s)
            f.close()

    #  Print Idle times:
    for strategy in strategies:
        for alpha in alphas:
            filename = 'idle_' + str(strategy) + '_' + str(alpha)
            f = open(filename, 'w')
            f.write('# Idle times over time\n')
            f.write('#\n')
            f.write('# Time     Idle percentage\n')
            idle_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                               {'name': strategy, 'service_budget': service_budget, 'alpha': alpha}, 3,
                                               'LATENCY', 'IDLE_TIMES')
            for k in sorted(sat_times):
                s = str(k[0][0]) + "\t" + str((1.0 * k[1])) + "\n"
                f.write(s)
            f.close()


def print_budget_experiment(lst):
    strategies = ['COORDINATED', 'HYBRID', 'HYBRIDS_REPO_APP']
    TREE_DEPTH = 3
    BRANCH_FACTOR = 2
    NUM_CORES = 50
    NUM_NODES = int(pow(BRANCH_FACTOR, TREE_DEPTH + 1) - 1)
    alpha = 0.75
    replacement_interval = 30.0
    N_SERVICES = 1000
    budgets = [NUM_CORES * NUM_NODES * 1, NUM_CORES * NUM_NODES * 3 / 2, NUM_CORES * NUM_NODES * 2,
               NUM_CORES * NUM_NODES * 5 / 2, NUM_CORES * NUM_NODES * 3]

    #  Print Sat. rates:
    for strategy in strategies:
        for budget in budgets:
            filename = 'Results/VM_Budget_Results/' + 'sat_' + str(strategy) + '_' + str(budget)
            f = open(filename, 'w')
            f.write('# Sat. rate over time\n')
            f.write('#\n')
            f.write('# Time     Sat. Rate\n')
            sat_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                              {'name': strategy, 'service_budget': budget, 'alpha': alpha}, 3,
                                              'LATENCY', 'SAT_TIMES')
            for k in sorted(sat_times):
                s = str(k[0][0]) + "\t" + str(k[1]) + "\n"
                f.write(s)
            f.close()

    #  Print Idle times:
    for strategy in strategies:
        for budget in budgets:
            filename = 'Results/VM_Budget_Results/' + 'idle_' + str(strategy) + '_' + str(budget)
            f = open(filename, 'w')
            f.write('# Idle times over time\n')
            f.write('#\n')
            f.write('# Time     Idle percentage\n')
            idle_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                               {'name': strategy, 'service_budget': budget, 'alpha': alpha}, 3,
                                               'LATENCY', 'IDLE_TIMES')
            for k in sorted(idle_times):
                s = str(k[0][0]) + "\t" + str((1.0 * k[1])) + "\n"
                f.write(s)
            f.close()

    #  Print Overhead times:
    for strategy in strategies:
        for budget in budgets:
            filename = 'Results/VM_Budget_Results/' + 'overhead_' + str(strategy) + '_' + str(budget)
            f = open(filename, 'w')
            f.write('# VM instantiation overhead over time\n')
            f.write('#\n')
            f.write('# Time     Overhead\n')
            overhead_times = searchDictMultipleCat(lst, ['strategy', 'computation_placement', 'workload'],
                                                   {'name': strategy, 'service_budget': budget, 'alpha': alpha}, 3,
                                                   'LATENCY', 'INSTANTIATION_OVERHEAD')
            for k in sorted(overhead_times):
                s = str(k[0][0]) + "\t" + str((1.0 * k[1])) + "\n"
                f.write(s)
            f.close()


def printTree(tree, d=0):
    if (tree == None or len(tree) == 0):
        print "\t" * d, "-"
    else:
        for key, val in tree.items():
            if (isinstance(val, dict)):
                print "\t" * d, key
                printTree(val, d + 1)
            else:
                print "\t" * d, key, str(val)


def writeTree(tree, f, d=0):
    if (tree == None or len(tree) == 0):
        print "\t" * d, "-"
    else:
        for key, val in tree.items():
            if (isinstance(val, dict)):
                f.write("\t" * d, key)
                printTree(val, d + 1)
            else:
                f.write("\t" * d, key, str(val))


def run(config, results, plotdir):
    """Run the plot script

    Parameters
    ----------
    config : str
        The path of the configuration file
    results : str
        The file storing the experiment results
    plotdir : str
        The directory into which graphs will be saved
    """
    resultset = RESULTS_READER['PICKLE'](results)
    # Onur: added this BEGIN
    lst = resultset.dump()
    f = open("raw_results.txt", "w")
    for l in lst:
        print 'PARAMETERS:\n'
        printTree(l[0])
        print 'RESULTS:\n'
        printTree(l[1])

        f.write('PARAMETERS:\n')
        writeTree(l[0], f)
        f.write('RESULTS:\n')
        writeTree(l[1], f)

    # print_lru_probability_results(lst)

    # print_strategies_performance(lst)
    print_budget_experiment(lst)
    # print_scheduling_experiments(lst)
    # print_zipf_experiment(lst)

    # /home/uceeoas/.local/bin/python ./plotresults.py --results results.pickle --output ./ config.py
    """
    settings = Settings()
    settings.read_from(config)
    config_logging(settings.LOG_LEVEL)
    resultset = RESULTS_READER[settings.RESULTS_FORMAT](results)
    # Create dir if not existsing
    if not os.path.exists(plotdir):
        os.makedirs(plotdir)
    # Parse params from settings
    topologies = settings.TOPOLOGIES
    cache_sizes = settings.NETWORK_CACHE
    alphas = settings.ALPHA
    strategies = settings.STRATEGIES
    # Plot graphs
    for topology in topologies:
        for cache_size in cache_sizes:
            logger.info('Plotting cache hit ratio for topology %s and cache size %s vs alpha' % (topology, str(cache_size)))
            plot_cache_hits_vs_alpha(resultset, topology, cache_size, alphas, strategies, plotdir)
            logger.info('Plotting link load for topology %s vs cache size %s' % (topology, str(cache_size)))
            plot_link_load_vs_alpha(resultset, topology, cache_size, alphas, strategies, plotdir)
            logger.info('Plotting latency for topology %s vs cache size %s' % (topology, str(cache_size)))
            plot_latency_vs_alpha(resultset, topology, cache_size, alphas, strategies, plotdir)
    for topology in topologies:
        for alpha in alphas:
            logger.info('Plotting cache hit ratio for topology %s and alpha %s vs cache size' % (topology, str(alpha)))
            plot_cache_hits_vs_cache_size(resultset, topology, alpha, cache_sizes, strategies, plotdir)
            logger.info('Plotting link load for topology %s and alpha %s vs cache size' % (topology, str(alpha)))
            plot_link_load_vs_cache_size(resultset, topology, alpha, cache_sizes, strategies, plotdir)
            logger.info('Plotting latency for topology %s and alpha %s vs cache size' % (topology, str(alpha)))
            plot_latency_vs_cache_size(resultset, topology, alpha, cache_sizes, strategies, plotdir)
    for cache_size in cache_sizes:
        for alpha in alphas:
            logger.info('Plotting cache hit ratio for cache size %s vs alpha %s against topologies' % (str(cache_size), str(alpha)))
            plot_cache_hits_vs_topology(resultset, alpha, cache_size, topologies, strategies, plotdir)
            logger.info('Plotting link load for cache size %s vs alpha %s against topologies' % (str(cache_size), str(alpha)))
            plot_link_load_vs_topology(resultset, alpha, cache_size, topologies, strategies, plotdir)
    logger.info('Exit. Plots were saved in directory %s' % os.path.abspath(plotdir))
    """


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("-r", "--results", dest="results",
                        help='the results file',
                        required=True)
    parser.add_argument("-o", "--output", dest="output",
                        help='the output directory where plots will be saved',
                        required=True)
    parser.add_argument("config",
                        help="the configuration file")
    args = parser.parse_args()
    run(args.config, args.results, args.output)


if __name__ == '__main__':
    main()
