# -*- coding: utf-8 -*-
"""This module contains all configuration information used to run simulations
"""
from multiprocessing import cpu_count
from collections import deque
import copy
from math import pow
from icarus.util import Tree

# GENERAL SETTINGS

# Level of logging output
# Available options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = 'INFO'

# If True, executes simulations in parallel using multiple processes
# to take advantage of multicore CPUs
PARALLEL_EXECUTION = True

# Number of processes used to run simulations in parallel.
# This option is ignored if PARALLEL_EXECUTION = False
N_PROCESSES = 10 #cpu_count()

# Granularity of caching.
# Currently, only OBJECT is supported
CACHING_GRANULARITY = 'OBJECT'

# Warm-up strategy
#WARMUP_STRATEGY = 'MFU' #'HYBRID'
WARMUP_STRATEGY = 'HYBRID' #'HYBRID'

# Format in which results are saved.
# Result readers and writers are located in module ./icarus/results/readwrite.py
# Currently only PICKLE is supported 
RESULTS_FORMAT = 'PICKLE'

# Number of times each experiment is replicated
# This is necessary for extracting confidence interval of selected metrics
N_REPLICATIONS = 1

# List of metrics to be measured in the experiments
# The implementation of data collectors are located in ./icaurs/execution/collectors.py
DATA_COLLECTORS = ['LATENCY']

# Range of alpha values of the Zipf distribution using to generate content requests
# alpha values must be positive. The greater the value the more skewed is the 
# content popularity distribution
# Range of alpha values of the Zipf distribution using to generate content requests
# alpha values must be positive. The greater the value the more skewed is the 
# content popularity distribution
# Note: to generate these alpha values, numpy.arange could also be used, but it
# is not recommended because generated numbers may be not those desired. 
# E.g. arange may return 0.799999999999 instead of 0.8. 
# This would give problems while trying to plot the results because if for
# example I wanted to filter experiment with alpha=0.8, experiments with
# alpha = 0.799999999999 would not be recognized 
ALPHA = 0.75 #0.75
#ALPHA = [0.00001]

# Total size of network cache as a fraction of content population
NETWORK_CACHE = 0.05

# Number of content objects
#N_CONTENTS = 1000
N_CONTENTS = 1000

N_SERVICES = N_CONTENTS

# Number of requests per second (over the whole network)
NETWORK_REQUEST_RATE = 10000.0

# Number of cores for each node in the experiment
NUM_CORES = 50

# Number of content requests generated to prepopulate the caches
# These requests are not logged
N_WARMUP_REQUESTS = 0 #30000

# Number of content requests generated after the warmup and logged
# to generate results. 
#N_MEASURED_REQUESTS = 1000 #60*30000 #100000

SECS = 60 #do not change
MINS = 3.0 #10.0 #5.5
N_MEASURED_REQUESTS = NETWORK_REQUEST_RATE*SECS*MINS

# List of all implemented topologies
# Topology implementations are located in ./icarus/scenarios/topology.py
TOPOLOGIES =  ['TREE']
TREE_DEPTH = 3
BRANCH_FACTOR = 2
NUM_NODES = int(pow(BRANCH_FACTOR, TREE_DEPTH+1) -1) 

# Replacement Interval in seconds
REPLACEMENT_INTERVAL = 30.0
NUM_REPLACEMENTS = 10000

# List of caching and routing strategies
# The code is located in ./icarus/models/strategy.py
STRATEGIES = ['COORDINATED', 'SDF', 'HYBRID', 'MFU', 'OPTIMAL_PLACEMENT_SCHED']  # service-based routing
STRATEGIES = ['OPTIMAL_PLACEMENT_SCHED']  # service-based routing

# Cache replacement policy used by the network caches.
# Supported policies are: 'LRU', 'LFU', 'FIFO', 'RAND' and 'NULL'
# Cache policy implmentations are located in ./icarus/models/cache.py
CACHE_POLICY = 'LRU'

# Task scheduling policy used by the cloudlets.
# Supported policies are: 'EDF' (Earliest Deadline First), 'FIFO'
SCHED_POLICY = 'EDF'

# Queue of experiments
EXPERIMENT_QUEUE = deque()
default = Tree()

default['workload'] = {'name':       'STATIONARY',
                       'n_contents': N_CONTENTS,
                       'n_warmup':   N_WARMUP_REQUESTS,
                       'n_measured': N_MEASURED_REQUESTS,
                       'rate':       NETWORK_REQUEST_RATE,
                       'seed':  0,
                       'n_services': N_SERVICES,
                       'alpha' : ALPHA
                      }
default['cache_placement']['name'] = 'UNIFORM'
#default['computation_placement']['name'] = 'CENTRALITY'
default['computation_placement']['name'] = 'UNIFORM'
#default['computation_placement']['name'] = 'CENTRALITY'
default['computation_placement']['service_budget'] = NUM_CORES*NUM_NODES*3 #   N_SERVICES/2 #N_SERVICES/2 
default['cache_placement']['network_cache'] = default['computation_placement']['service_budget']
default['computation_placement']['computation_budget'] = (NUM_NODES)*NUM_CORES  # NUM_CORES for each node 
default['content_placement']['name'] = 'UNIFORM'
default['cache_policy']['name'] = CACHE_POLICY
default['sched_policy']['name'] = SCHED_POLICY
default['strategy']['replacement_interval'] = REPLACEMENT_INTERVAL
default['strategy']['n_replacements'] = NUM_REPLACEMENTS
default['topology']['name'] = 'TREE'
default['topology']['k'] = BRANCH_FACTOR
default['topology']['h'] = TREE_DEPTH
default['warmup_strategy']['name'] = WARMUP_STRATEGY

# Create experiments multiplexing all desired parameters
"""
for strategy in ['LRU']: # STRATEGIES:
    for p in [0.1, 0.25, 0.50, 0.75, 1.0]:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['strategy']['p'] = p
        experiment['desc'] = "strategy: %s, prob: %s" \
                             % (strategy, str(p))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Compare SDF, LFU, Hybrid for default values
#"""
for strategy in STRATEGIES:
    experiment = copy.deepcopy(default)
    if strategy == 'COORDINATED':
        default['computation_placement']['service_budget'] = NUM_NODES*N_SERVICES*NUM_CORES
    experiment['strategy']['name'] = strategy
    experiment['warmup_strategy']['name'] = strategy
    experiment['desc'] = "strategy: %s" \
                         % (strategy)
    EXPERIMENT_QUEUE.append(experiment)
#"""
# Experiment with different budgets
"""
budgets = [N_SERVICES/8, N_SERVICES/4, N_SERVICES/2, 0.75*N_SERVICES, N_SERVICES, 2*N_SERVICES]
for strategy in STRATEGIES:
    for budget in budgets:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['computation_placement']['service_budget'] = budget
        experiment['strategy']['replacement_interval'] = REPLACEMENT_INTERVAL
        experiment['strategy']['n_replacements'] = NUM_REPLACEMENTS
        experiment['desc'] = "strategy: %s, budget: %s" \
                             % (strategy, str(budget))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment comparing FIFO with EDF 
"""
for schedule_policy in ['EDF', 'FIFO']:
    for strategy in STRATEGIES:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['sched_policy']['name'] = schedule_policy
        experiment['desc'] = "strategy: %s, schedule policy: %s" \
                             % (strategy, str(schedule_policy))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment with various zipf values
"""
for alpha in [0.1, 0.25, 0.50, 0.75, 1.0]:
    for strategy in STRATEGIES:
        experiment = copy.deepcopy(default)
        experiment['workload']['alpha'] = alpha
        experiment['strategy']['name'] = strategy
        experiment['desc'] = "strategy: %s, zipf: %s" \
                         % (strategy, str(alpha))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment with various request rates (for sanity checking)
