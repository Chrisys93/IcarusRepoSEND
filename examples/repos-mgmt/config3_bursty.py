# -*- codi# -*- coding: utf-8 -*-
"""This module contains all configuration information used to run simulations
"""
from multiprocessing import cpu_count
from collections import deque
import copy
import os
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
RESULTS_FORMAT = 'TXT'

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
N_CONTENTS = 500
#N_CONTENTS = 1000

N_SERVICES = N_CONTENTS

# Number of requests per second (over the whole network)
NETWORK_REQUEST_RATE = 100.0

# Number of cores for each node in the experiment
NUM_CORES = 50

# Number of content requests generated to prepopulate the caches
# These requests are not logged
N_WARMUP_REQUESTS = 0 #30000

# Number of content requests generated after the warmup and logged
# to generate results.
#N_MEASURED_REQUESTS = 1000 #60*30000 #100000

SECS = 60 #do not change
MINS = 7.5 #5.5
N_MEASURED_REQUESTS = NETWORK_REQUEST_RATE*SECS*MINS

# List of all implemented topologies
# Topology implementations are located in ./icarus/scenarios/topology.py
TOPOLOGIES =  ['TREE']
TREE_DEPTH = 3
BRANCH_FACTOR = 2
NUM_NODES = int(pow(BRANCH_FACTOR, TREE_DEPTH+1) -1)

# Replacement Interval in seconds
REPLACEMENT_INTERVAL = 30.0
NUM_REPLACEMENTS = 5000

# List of workloads that generate the request rates
# The code is located in ./icarus/scenatios
WORKLOAD = 'BURSTY_MORE_LABEL_REQS'

# List of caching and routing strategies
# The code is located in ./icarus/models/strategy.py
STRATEGIES = ['HYBRIDS_RE_REPO_APP', 'HYBRIDS_SPEC_REPO_APP']
#STRATEGIES = ['COORDINATED']  # service-based routing

# Cache replacement policy used by the network caches.
# Supported policies are: 'LRU', 'LFU', 'FIFO', 'RAND' and 'NULL'
# Cache policy implmentations are located in ./icarus/models/cache.py
CACHE_POLICY = 'LRU'

# Repo replacement policy used by the network repositories.
# Supported policy is the normal, demonstrated REPO_STORAGE and NULL_REPO data storage policy
# Cache policy implmentations are located in ./icarus/models/repo.py
REPO_POLICY = 'REPO_STORAGE'

# Task scheduling policy used by the cloudlets.
# Supported policies are: 'EDF' (Earliest Deadline First), 'FIFO'
SCHED_POLICY = 'EDF'

FRESHNESS_PER = 0.15
SHELF_LIFE = 5
MSG_SIZE = 1000000
SOURCE_WEIGHTS = {'src_0': 0.2, 7: 0.1, 8: 0.1, 10: 0.2, 11: 0.2, 13: 0.1, 14: 0.1}
SERVICE_WEIGHTS = {"proc": 0.7, "non-proc": 0.3}
TYPES_WEIGHTS = {"value": 0.3, "video": 0.2, "control": 0.1, "photo": 0.2, "audio": 0.2}
TOPICS_WEIGHTS = {"traffic": 0.3, "home_IoT": 0.3, "office_IoT": 0.2, "security": 0.2}
MAX_REQUESTED_LABELS = 3
MAX_REPLICATIONS = None
ALPHA_LABELS = 0.5
DATA_TOPICS = ["traffic", "home_IoT", "office_IoT", "security"]
DATA_TYPES = ["value", "video", "control", "photo", "audio"]
LABEL_EXCL = False

# Files for workload:
dir_path = os.path.realpath('./')
RATES_FILE = dir_path + 'target_and_rates.csv'
CONTENTS_FILE = dir_path + 'contents.csv'
LABELS_FILE = dir_path + 'labels.csv'
CONTENT_LOCATIONS = dir_path + 'content_locations.csv'

# Bursty workload settings:
# Maximum times for online and offline request generation
MAX_ON = NETWORK_REQUEST_RATE/2
MAX_OFF = NETWORK_REQUEST_RATE
# Mode of receiver network connection disruption (Available: None, 'RAND', 'WEIGHTED')
DISRUPT_MODE = 'RAND'
# Only for WEIGHTED mode, above, otherwise None - weights of receivers' off-time distribution
DISRUPTION_WEIGHTS = None # {'rec_0': 0.2, rec_1: 0.1, rec_2: 0.1, rec_4: 0.2, rec_5: 0.2, rec_6: 0.1, rec_7: 0.1}


# Queue of experiments
EXPERIMENT_QUEUE = deque()
default = Tree()

default['workload'] = {'name': WORKLOAD,
                       'n_contents': N_CONTENTS,
                       'n_warmup': N_WARMUP_REQUESTS,
                       'n_measured': N_MEASURED_REQUESTS,
                       'rate': NETWORK_REQUEST_RATE,
                       'seed': 0,
                       'n_services': N_SERVICES,
                       'alpha': ALPHA,
                       'alpha_labels': ALPHA_LABELS,
                       'topics': DATA_TOPICS,
                       'label_ex': LABEL_EXCL,
                       'types': DATA_TYPES,
                       'max_labels': MAX_REQUESTED_LABELS,
                       'freshness_pers': FRESHNESS_PER,
                       'shelf_lives': SHELF_LIFE,
                       'msg_sizes': MSG_SIZE,
                       'rates_file': RATES_FILE,
                       'contents_file': CONTENTS_FILE,
                       'labels_file': LABELS_FILE,
                       'content_locations': CONTENT_LOCATIONS,
                       'max_on': MAX_ON,
                       'max_off': MAX_OFF,
                       'disrupt_mode': DISRUPT_MODE,
                       'disrupt_weights': DISRUPTION_WEIGHTS
                       }

default['cache_placement']['name'] = 'CONSOLIDATED_REPO_CACHE'
default['cache_placement']['storage_budget'] = 10000000000
#default['computation_placement']['name'] = 'CENTRALITY'
default['computation_placement']['name'] = 'UNIFORM_REPO'
#default['computation_placement']['name'] = 'CENTRALITY'
default['computation_placement']['service_budget'] = NUM_CORES*NUM_NODES*3 #   N_SERVICES/2 #N_SERVICES/2
default['computation_placement']['storage_budget'] = 10000000000
default['cache_placement']['network_cache'] = default['computation_placement']['service_budget']
default['computation_placement']['computation_budget'] = (NUM_NODES)*NUM_CORES  # NUM_CORES for each node
#default['content_placement']['name'] = 'WEIGHTED_REPO'

default['content_placement'] = {"name":             'WEIGHTED_REPO',
                                "topics_weights" :  TOPICS_WEIGHTS,
                                "types_weights" :   TYPES_WEIGHTS,
                                "max_replications": MAX_REPLICATIONS,
                                "source_weights" :  SOURCE_WEIGHTS,
                                "service_weights":  SERVICE_WEIGHTS,
                                "max_label_nos" :   MAX_REQUESTED_LABELS
                                }

default['cache_policy']['name'] = CACHE_POLICY
default['repo_policy']['name'] = REPO_POLICY
default['sched_policy']['name'] = SCHED_POLICY
default['strategy']['replacement_interval'] = REPLACEMENT_INTERVAL
default['strategy']['n_replacements'] = NUM_REPLACEMENTS
default['topology']['name'] = 'REPO_TREE'
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
# TODO: Add workloads - Furthermore, we don't need service budget variations here!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
SERVICE_BUDGET = NUM_CORES*NUM_NODES*3
for strategy in STRATEGIES:
    experiment = copy.deepcopy(default)
    experiment['computation_placement']['service_budget'] = SERVICE_BUDGET
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
