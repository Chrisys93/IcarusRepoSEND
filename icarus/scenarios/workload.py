# -*- coding: utf-8 -*-
"""Traffic workloads

Every traffic workload to be used with Icarus must be modelled as an iterable
class, i.e. a class with at least an `__init__` method (through which it is
initialized, with values taken from the configuration file) and an `__iter__`
method that is called to return a new event.

Each call to the `__iter__` method must return a 2-tuple in which the first
element is the timestamp at which the event occurs and the second is a
dictionary, describing the event, which must contain at least the three
following attributes:
 * receiver: The name of the node issuing the request
 * content: The name of the content for which the request is issued
 * log: A boolean value indicating whether this request should be logged or not
   for measurement purposes.

Each workload must expose the `contents` attribute which is an iterable of
all content identifiers. This is needed for content placement.
"""
import random
import csv

import networkx as nx
import heapq

from fnss.util import random_from_pdf
from icarus.tools import TruncatedZipfDist
from icarus.registry import register_workload
from collections import Counter

__all__ = [
    'StationaryWorkload',
    'RepoStationaryWorkload',
    'GlobetraffWorkload',
    'TraceDrivenWorkload',
    'YCSBWorkload',
    'StationaryRepoWorkload',
    'BurstyRepoWorkload',
    'TraceDrivenRepoWorkload',
    'BurstyTraceRepoWorkload',
    'BurstyRepoDataAndWorkload'
]

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
STORE = 3


@register_workload('STATIONARY')
class StationaryWorkload(object):
    """This function generates events on the fly, i.e. instead of creating an
    event schedule to be kept in memory, returns an iterator that generates
    events when needed.

    This is useful for running large schedules of events where RAM is limited
    as its memory impact is considerably lower.

    These requests are Poisson-distributed while content popularity is
    Zipf-distributed

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    n_contents : int
        The number of content object
    alpha : float
        The Zipf alpha parameter
    beta : float, optional
        Parameter indicating
    rate : float, optional
        The mean rate of requests per second
    n_warmup : int, optional
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int, optional
        The number of logged requests after the warmup

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, n_contents, alpha, beta=0, rate=1.0,
                 n_warmup=10 ** 5, n_measured=4 * 10 ** 5, seed=0,
                 n_services=10, **kwargs):
        if alpha < 0:
            raise ValueError('alpha must be positive')
        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes() if 'receiver' in topology.nodes[v]['stack'][0]]
        self.zipf = TruncatedZipfDist(alpha, n_services - 1, seed)

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = range(0, n_contents)
        self.n_services = n_services
        self.alpha = alpha
        self.rate = rate
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.adj[x])], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

        self.seed = seed
        self.first = True

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        if self.first:  # TODO remove this first variable, this is not necessary here
            random.seed(self.seed)
            self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'log': log,
                         'labels': eventObj.labels, 'node': eventObj.node, 'flow_id': eventObj.flow_id,
                         'deadline': eventObj.deadline, 'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status,
                         'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!

            if type(content) is not dict:
                data = dict()
                data.update(content=content)
            else:
                data = content
            data.update(labels=[])
            data.update(service_type="proc")

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            deadline = self.model.services[content].deadline + t_event
            event = {'receiver': receiver, 'content': data, 'log': log, 'labels': data['labels'], 'node': node,
                     'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            neighbors = self.topology.neighbors(receiver)
            # s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print ("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()


@register_workload('REPO_STATIONARY')
class RepoStationaryWorkload(object):
    """This function generates events on the fly, i.e. instead of creating an
    event schedule to be kept in memory, returns an iterator that generates
    events when needed.

    This is useful for running large schedules of events where RAM is limited
    as its memory impact is considerably lower.

    These requests are Poisson-distributed while content popularity is
    Zipf-distributed

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    n_contents : int
        The number of content object
    alpha : float
        The Zipf alpha parameter
    beta : float, optional
        Parameter indicating
    rate : float, optional
        The mean rate of requests per second
    n_warmup : int, optional
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int, optional
        The number of logged requests after the warmup

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, n_contents, alpha, beta=0, rate=1.0,
                 n_warmup=10 ** 5, n_measured=4 * 10 ** 5, seed=0,
                 n_services=10, **kwargs):
        if alpha < 0:
            raise ValueError('alpha must be positive')
        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes() if 'receiver' in topology.nodes[v]['stack'][0]]
        self.zipf = TruncatedZipfDist(alpha, n_services - 1, seed)

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = {}
        for content in range(0, n_contents):
            if self.contents.has_key(content):
                self.contents[content].update(None)
            else:
                self.contents[content] = None

        self.n_services = n_services
        self.alpha = alpha
        self.rate = rate
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.adj[x])], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

        self.seed = seed
        self.first = True

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        if self.first:  # TODO remove this first variable, this is not necessary here
            random.seed(self.seed)
            self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'log': log,
                         'labels': eventObj.labels, 'node': eventObj.node, 'flow_id': eventObj.flow_id,
                         'deadline': eventObj.deadline, 'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status,
                         'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!
            labels = []

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            deadline = self.model.services[content].deadline + t_event
            event = {'receiver': receiver, 'content': content, 'log': log, 'labels': labels, 'node': node,
                     'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            neighbors = self.topology.neighbors(receiver)
            # s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print ("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()


@register_workload('GLOBETRAFF')
class GlobetraffWorkload(object):
    """Parse requests from GlobeTraff workload generator

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    reqs_file : str
        The GlobeTraff request file
    contents_file : str
        The GlobeTraff content file
    beta : float, optional
        Spatial skewness of requests rates

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, reqs_file, contents_file, beta=0, **kwargs):
        """Constructor"""
        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.n_contents = 0
        with open(contents_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for content, popularity, size, app_type in reader:
                self.n_contents = max(self.n_contents, content)
        self.n_contents += 1
        self.contents = range(self.n_contents)
        self.request_file = reqs_file
        self.beta = beta
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x:
            degree[iter(topology.edge[x]).next()],
                                    reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

    def __iter__(self):
        with open(self.request_file, 'r') as f:
            reader = csv.reader(f, delimiter='\t')
            for timestamp, content, size in reader:
                if self.beta == 0:
                    receiver = random.choice(self.receivers)
                else:
                    receiver = self.receivers[self.receiver_dist.rv() - 1]
                event = {'receiver': receiver, 'content': content, 'size': size}
                yield (timestamp, event)
        raise StopIteration()


@register_workload('TRACE_DRIVEN')
class TraceDrivenWorkload(object):
    """Parse requests from a generic request trace.

    TODO: Maybe check this for modifications, as well

    This workload requires two text files:
     * a requests file, where each line corresponds to a string identifying
       the content requested
     * a contents file, which lists all unique content identifiers appearing
       in the requests file.

    Since the trace do not provide timestamps, requests are scheduled according
    to a Poisson process of rate *rate*. All requests are mapped to receivers
    uniformly unless a positive *beta* parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    reqs_file : str
        The path to the requests file
    contents_file : str
        The path to the contents file
    n_contents : int
        The number of content object (i.e. the number of lines of contents_file)
    n_warmup : int
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int
        The number of logged requests after the warmup
    rate : float, optional
        The network-wide mean rate of requests per second
    beta : float, optional
        Spatial skewness of requests rates

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, reqs_file, contents_file, n_contents,
                 n_warmup, n_measured, rate=1.0, beta=0, **kwargs):
        """Constructor"""
        if beta < 0:
            raise ValueError('beta must be positive')
        # Set high buffering to avoid one-line reads
        self.buffering = 64 * 1024 * 1024
        self.n_contents = n_contents
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.reqs_file = reqs_file
        self.rate = rate
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.contents = []
        with open(contents_file, 'r', buffering=self.buffering) as f:
            for content in f:
                self.contents.append(content)
        self.beta = beta
        if beta != 0:
            degree = nx.degree(topology)
            self.receivers = sorted(self.receivers, key=lambda x:
            degree[iter(topology.edge[x]).next()],
                                    reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        with open(self.reqs_file, 'r', buffering=self.buffering) as f:
            for content in f:
                t_event += (random.expovariate(self.rate))
                if self.beta == 0:
                    receiver = random.choice(self.receivers)
                else:
                    receiver = self.receivers[self.receiver_dist.rv() - 1]
                log = (req_counter >= self.n_warmup)
                event = {'receiver': receiver, 'content': content, 'log': log}
                yield (t_event, event)
                req_counter += 1
                if (req_counter >= self.n_warmup + self.n_measured):
                    raise StopIteration()
            raise ValueError("Trace did not contain enough requests")


@register_workload('YCSB')
class YCSBWorkload(object):
    """Yahoo! Cloud Serving Benchmark (YCSB)

    The YCSB is a set of reference workloads used to benchmark databases and,
    more generally any storage/caching systems. It comprises five workloads:

    +------------------+------------------------+------------------+
    | Workload         | Operations             | Record selection |
    +------------------+------------------------+------------------+
    | A - Update heavy | Read: 50%, Update: 50% | Zipfian          |
    | B - Read heavy   | Read: 95%, Update: 5%  | Zipfian          |
    | C - Read only    | Read: 100%             | Zipfian          |
    | D - Read latest  | Read: 95%, Insert: 5%  | Latest           |
    | E - Short ranges | Scan: 95%, Insert 5%   | Zipfian/Uniform  |
    +------------------+------------------------+------------------+

    Notes
    -----
    At the moment only workloads A, B and C are implemented, since they are the
    most relevant for caching systems.
    """

    def __init__(self, workload, n_contents, n_warmup, n_measured, alpha=0.99, seed=None, **kwargs):
        """Constructor

        Parameters
        ----------
        workload : str
            Workload identifier. Currently supported: "A", "B", "C"
        n_contents : int
            Number of content items
        n_warmup : int, optional
            The number of warmup requests (i.e. requests executed to fill cache but
            not logged)
        n_measured : int, optional
            The number of logged requests after the warmup
        alpha : float, optional
            Parameter of Zipf distribution
        seed : int, optional
            The seed for the random generator
        """

        if workload not in ("A", "B", "C", "D", "E"):
            raise ValueError("Incorrect workload ID [A-B-C-D-E]")
        elif workload in ("D", "E"):
            raise NotImplementedError("Workloads D and E not yet implemented")
        self.workload = workload
        if seed is not None:
            random.seed(seed)
        self.zipf = TruncatedZipfDist(alpha, n_contents)
        self.n_warmup = n_warmup
        self.n_measured = n_measured

    def __iter__(self):
        """Return an iterator over the workload"""
        req_counter = 0
        while req_counter < self.n_warmup + self.n_measured:
            rand = random.random()
            op = {
                "A": "READ" if rand < 0.5 else "UPDATE",
                "B": "READ" if rand < 0.95 else "UPDATE",
                "C": "READ"
            }[self.workload]
            item = int(self.zipf.rv())
            log = (req_counter >= self.n_warmup)
            event = {'op': op, 'item': item, 'log': log}
            yield event
            req_counter += 1
        raise StopIteration()


@register_workload('STATIONARY_MORE_LABEL_REQS')
class StationaryRepoWorkload(object):
    """This function generates events on the fly, i.e. instead of creating an
    event schedule to be kept in memory, returns an iterator that generates
    events when needed.

    This is useful for running large schedules of events where RAM is limited
    as its memory impact is considerably lower.

    These requests are Poisson-distributed while content popularity is
    Zipf-distributed

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    n_contents : int
        The number of content object
    alpha : float
        The Zipf alpha parameter
    beta : float, optional
        Parameter indicating
    rate : float, optional
        The mean rate of requests per second
    n_warmup : int, optional
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int, optional
        The number of logged requests after the warmup

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, n_contents, alpha, beta=0,
                 label_ex=False, alpha_labels=0, rate=1.0,
                 n_warmup=10 ** 5, n_measured=4 * 10 ** 5, seed=0,
                 n_services=10, topics=None, types=None, max_labels=1,
                 freshness_pers=0, shelf_lives=0, msg_sizes=1000000, **kwargs):
        if types is None:
            types = []
        if alpha < 0:
            raise ValueError('alpha must be positive')

        if alpha_labels < 0:
            raise ValueError('alpha_labels must be positive')

        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.zipf = TruncatedZipfDist(alpha, n_services - 1, seed)
        self.labels_zipf = TruncatedZipfDist(alpha_labels, len(topics) + len(types), seed)

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # TODO: Associate all below content properties to contents, according to CONFIGURATION required IN FILE, in
        #  contentplacement.py file, registered placement strategies!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = range(0, n_contents)
        self.data = dict()

        for content in self.contents:
            if type(content) is not dict:
                datum = dict()
                datum.update(content=content)
            else:
                datum = content
            datum.update(service_type="proc")
            datum.update(labels=[])
            datum.update(msg_size=msg_sizes)
            datum.update(shelf_life=[])
            datum.update(freshness_per=freshness_pers)
            self.data[content] = datum

        self.labels = dict(topics=topics,
                           types=types)
        self.freshness_pers = freshness_pers
        self.shelf_lives = shelf_lives
        self.sizes = msg_sizes

        self.n_services = n_services
        self.alpha = alpha
        self.alpha_labels = alpha_labels
        self.max_labels = max_labels
        self.label_ex = label_ex
        self.alter = False
        self.rate = rate
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.edge[x]).next()], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers), seed)

        self.seed = seed
        self.first = True

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        if self.first:  # TODO remove this first variable, this is not necessary here
            random.seed(self.seed)
            self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            labels = []
            content = None
            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                    labels_zipf = int(self.labels_zipf.rv())
                    if labels_zipf < len(self.labels['topics']):
                        labels.append(self.labels['topics'][labels_zipf])
                    elif labels_zipf >= len(self.labels['topics']):
                        labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])-1])

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels_zipf = int(self.labels_zipf.rv())
                        if labels_zipf < len(self.labels['topics']):
                            labels.append(self.labels['topics'][labels_zipf])
                        elif labels_zipf >= len(self.labels['topics']):
                            labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])])
                    self.alter = True
                    content = int(self.zipf.rv())

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_event
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            # el
            if labels:

                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                datum.update(labels=labels)

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_event
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            else:
                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                deadline = self.model.services[content].deadline + t_event
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            #s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()




@register_workload('BURSTY_MORE_LABEL_REQS')
class BurstyRepoWorkload(object):
    """This function generates events on the fly, i.e. instead of creating an
    event schedule to be kept in memory, returns an iterator that generates
    events when needed.

    This is useful for running large schedules of events where RAM is limited
    as its memory impact is considerably lower.

    These requests are Poisson-distributed while content popularity is
    Zipf-distributed

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    n_contents : int
        The number of content object
    alpha : float
        The Zipf alpha parameter
    beta : float, optional
        Parameter indicating
    rate : float, optional
        The mean rate of requests per second
    n_warmup : int, optional
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int, optional
        The number of logged requests after the warmup

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, n_contents, alpha, max_on=0, max_off=0, disrupt_mode=None, disrupt_weights=None,
                 beta=0, label_ex=False, alpha_labels=0, rate=1.0, n_warmup=10 ** 5, n_measured=4 * 10 ** 5, seed=0,
                 n_services=10, topics=None, types=None, max_labels=1, freshness_pers=0, shelf_lives=0,
                 msg_sizes=1000000, **kwargs):
        if types is None:
            types = []
        if alpha < 0:
            raise ValueError('alpha must be positive')

        if alpha_labels < 0:
            raise ValueError('alpha_labels must be positive')

        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.zipf = TruncatedZipfDist(alpha, n_services - 1, seed)
        self.labels_zipf = TruncatedZipfDist(alpha_labels, len(topics) + len(types), seed)

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # TODO: Associate all below content properties to contents, according to CONFIGURATION required IN FILE, in
        #  contentplacement.py file, registered placement strategies!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = range(0, n_contents)
        self.data = dict()

        for content in self.contents:
            if type(content) is not dict:
                datum = dict()
                datum.update(content=content)
            else:
                datum = content
            datum.update(service_type="proc")
            datum.update(labels=[])
            datum.update(msg_size=msg_sizes)
            datum.update(shelf_life=[])
            datum.update(freshness_per=freshness_pers)
            self.data[content] = datum

        self.labels = dict(topics=topics,
                           types=types)
        self.freshness_pers = freshness_pers
        self.shelf_lives = shelf_lives
        self.sizes = msg_sizes

        self.n_services = n_services
        self.alpha = alpha
        self.alpha_labels = alpha_labels
        self.max_labels = max_labels
        self.label_ex = label_ex
        self.alter = False
        self.rate = rate
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology

        self.max_on = max_on
        self.max_off = max_off
        self.disrupt_mode = disrupt_mode
        self.disrupt_weights = disrupt_weights
        self.receivers_down = {}
        self.last_event = {}
        for receiver in self.receivers:
            self.receivers_down[receiver] = None
            self.last_event[receiver] = None
        self.receivers_downtime = {}
        self.receivers_uptime = {}
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.edge[x]).next()], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers), seed)

        self.seed = seed
        self.first = True

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        if self.first:  # TODO remove this first variable, this is not necessary here
            random.seed(self.seed)
            self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            if self.disrupt_mode == 'RAND':
                self.receivers_downtime[node] = random.uniform(0, self.max_off)
                self.receivers_uptime[node] = random.uniform(0, self.max_on)
                if self.last_event[node] is not None and self.receivers_down[node] is None:
                    self.receivers_down[node] = self.last_event[node] + self.receivers_downtime[node]
                elif self.receivers_down[node] and t_event < self.receivers_down[node]:
                    t_event = self.receivers_down[node]
                elif self.receivers_down[node] and t_event > self.receivers_down[node] + self.receivers_uptime[node]:
                    self.receivers_down[node] = None
                self.last_event[node] = t_event
                    
            

            labels = []
            content = None
            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                    labels_zipf = int(self.labels_zipf.rv())
                    if labels_zipf < len(self.labels['topics']):
                        labels.append(self.labels['topics'][labels_zipf])
                    elif labels_zipf >= len(self.labels['topics']):
                        labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])-1])

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels_zipf = int(self.labels_zipf.rv())
                        if labels_zipf < len(self.labels['topics']):
                            labels.append(self.labels['topics'][labels_zipf])
                        elif labels_zipf >= len(self.labels['topics']):
                            labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])])
                    self.alter = True
                    content = int(self.zipf.rv())

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_event
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            # el
            if labels:

                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                datum.update(labels=labels)

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_event
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            else:
                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                deadline = self.model.services[content].deadline + t_event
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            #s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()




@register_workload('TRACE_DRIVEN_REPO')
class TraceDrivenRepoWorkload(object):
    """Parse requests from a generic request trace.

    TODO: Maybe check this for modifications, as well

    This workload requires two text files:
     * a requests file, where each line corresponds to a string identifying
       the content requested
     * a contents file, which lists all unique content identifiers appearing
       in the requests file.

    Since the trace do not provide timestamps, requests are scheduled according
    to a Poisson process of rate *rate*. All requests are mapped to receivers
    uniformly unless a positive *beta* parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    reqs_file : str
        The path to the requests file
    contents_file : str
        The path to the contents file
    n_contents : int
        The number of content object (i.e. the number of lines of contents_file)
    n_warmup : int
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int
        The number of logged requests after the warmup
    rate : float, optional
        The network-wide mean rate of requests per second
    beta : float, optional
        Spatial skewness of requests rates

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, rates_file, contents_file, labels_file, content_locations, n_contents,
                 n_warmup, n_measured, n_services=10, max_labels=1, msg_sizes=1000000, freshness_pers=0.2,
                 shelf_lives=5, rate=1.0, label_ex=False, alpha_labels=0, beta=0, seed=0, **kwargs):
        """Constructor"""

        if alpha_labels < 0:
            raise ValueError('alpha_labels must be positive')

        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # TODO: Associate all below content properties to contents, according to CONFIGURATION required IN FILE, in
        #  contentplacement.py file, registered placement strategies!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        self.freshness_pers = freshness_pers
        self.shelf_lives = shelf_lives
        self.sizes = msg_sizes

        self.n_services = n_services
        self.alpha_labels = alpha_labels
        self.max_labels = max_labels
        self.label_ex = label_ex
        self.alter = False
        self.rate = rate
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.edge[x]).next()], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers), seed)

        self.seed = seed

        if beta < 0:
            raise ValueError('beta must be positive')
        # Set high buffering to avoid one-line reads
        self.buffering = 64 * 1024 * 1024
        self.n_contents = n_contents
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.contents_file = contents_file
        self.labels_file = labels_file
        self.content_locations =content_locations
        self.rates_file = rates_file
        self.coeffs = []
        with open(rates_file, 'r') as f:
            data = csv.reader(f)
            for co in data:
                first = True
                for coeff in co:
                    if first:
                        self.rate = float(coeff) #+ float(coeff)*0.1
                        first = False
                    else:
                        self.coeffs.append(float(coeff))
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.contents = []
        contents = []
        with open(self.contents_file, 'r', buffering=self.buffering) as f:
            data = csv.reader(f)
            for content in data:
                for cont in content:
                    contents.append(cont)
        self.labels = []
        labels = []
        with open(self.labels_file, 'r', buffering=self.buffering) as f:
            data = csv.reader(f)
            for label in data:
                for l in label:
                    labels.append(l)
        locations = []
        with open(self.content_locations, 'r', buffering=self.buffering) as loc:
            data = csv.reader(loc)
            for cont_loc in data:
                for c_loc in cont_loc:
                    locations.append(int(c_loc))
        alt = True
        no_contents = 0
        for location in locations:
            no_contents += 1
            self.contents.append(contents[location])
            if alt:
                self.labels.append(labels[location])
                alt = False
            else:
                alt = True
            if no_contents == n_contents:
                break

        unique_labels = []
        label_counts = Counter()
        for label in self.labels:
            if label not in unique_labels:
                unique_labels.append(label)
            label_counts.update([label])
        self.labels_pdf = {}
        self.labels_weights = {}
        for label in unique_labels:
            self.labels_weights[label] = label_counts[label]
        labels_norm_factor = float(sum(self.labels_weights.values()))
        self.labels_pdf = dict((k, v / labels_norm_factor) for k, v in self.labels_weights.items())

        for number in range(0, len(self.contents)):
            self.contents[number] = number
        self.rates_pdf = {}
        for content in self.contents:
            for i in range(0, len(self.contents)-1):
                if self.contents[i] == content:
                    self.rates_pdf[content] = self.coeffs[i]
        contents_weights = {}
        for content in self.rates_pdf:
            contents_weights[content] = self.rates_pdf[content]
        contents_norm_factor = float(sum(contents_weights.values()))
        self.rates_pdf = dict((k, v / contents_norm_factor) for k, v in contents_weights.items())


        self.data = dict()

        for content in self.contents:
            if type(content) is not dict:
                datum = dict()
                datum.update(content=content)
            else:
                datum = content
            datum.update(service_type="proc")
            datum.update(labels=[])
            datum.update(msg_size=msg_sizes)
            datum.update(shelf_life=[])
            datum.update(freshness_per=freshness_pers)
            self.data[content] = datum


        self.beta = beta
        if beta != 0:
            degree = nx.degree(topology)
            self.receivers = sorted(self.receivers, key=lambda x:
            degree[iter(topology.edge[x]).next()],
                                    reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        # if self.first:  # TODO remove this first variable, this is not necessary here - done!
        random.seed(self.seed)
        #    self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            labels = []
            content = None
            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                        labels.append(random_from_pdf(self.labels_pdf))

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(random_from_pdf(self.rates_pdf))  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels.append(random_from_pdf(self.labels_pdf))
                    self.alter = True
                    content = int(random_from_pdf(self.rates_pdf))

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_event
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            # el
            if labels:

                index = int(random_from_pdf(self.rates_pdf))
                datum = self.data[index]
                datum.update(service_type="proc")
                datum.update(labels=labels)
                self.data[index] = datum

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_event
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            else:
                index = int(random_from_pdf(self.rates_pdf))
                datum = self.data[index]
                datum.update(service_type="proc")
                deadline = self.model.services[content].deadline + t_event
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            #s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()

@register_workload('BURSTY_TRACE_DRIVEN_REPO')
class BurstyTraceRepoWorkload(object):
    """Parse requests from a generic request trace.

    TODO: Maybe check this for modifications, as well

    This workload requires two text files:
     * a requests file, where each line corresponds to a string identifying
       the content requested
     * a contents file, which lists all unique content identifiers appearing
       in the requests file.

    Since the trace do not provide timestamps, requests are scheduled according
    to a Poisson process of rate *rate*. All requests are mapped to receivers
    uniformly unless a positive *beta* parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    reqs_file : str
        The path to the requests file
    contents_file : str
        The path to the contents file
    n_contents : int
        The number of content object (i.e. the number of lines of contents_file)
    n_warmup : int
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int
        The number of logged requests after the warmup
    rate : float, optional
        The network-wide mean rate of requests per second
    beta : float, optional
        Spatial skewness of requests rates

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.
    """

    def __init__(self, topology, rates_file, contents_file, labels_file, content_locations, n_contents,
                 n_warmup, n_measured, max_on=0, max_off=0, disrupt_mode=None, disrupt_weights=None, n_services=10,
                 max_labels=1, msg_sizes=1000000, freshness_pers=0.2, shelf_lives=5, rate=1.0, label_ex=False,
                 alpha_labels=0, beta=0, seed=0, **kwargs):
        """Constructor"""

        if alpha_labels < 0:
            raise ValueError('alpha_labels must be positive')

        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # TODO: Associate all below content properties to contents, according to CONFIGURATION required IN FILE, in
        #  contentplacement.py file, registered placement strategies!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = range(0, n_contents)
        self.data = dict()

        for content in self.contents:
            if type(content) is not dict:
                datum = dict()
                datum.update(content=content)
            else:
                datum = content
            datum.update(service_type="proc")
            datum.update(labels=[])
            datum.update(msg_size=msg_sizes)
            datum.update(shelf_life=[])
            datum.update(freshness_per=freshness_pers)
            self.data[content] = datum

        self.freshness_pers = freshness_pers
        self.shelf_lives = shelf_lives
        self.sizes = msg_sizes

        self.n_services = n_services
        self.alpha_labels = alpha_labels
        self.max_labels = max_labels
        self.label_ex = label_ex
        self.alter = False
        self.rate = rate
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology
        self.max_on = max_on
        self.max_off = max_off
        self.disrupt_mode = disrupt_mode
        if self.disrupt_mode == 'WEIGHTED':
            self.disrupt_weights = disrupt_weights
        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.edge[x]).next()], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers), seed)

        self.seed = seed

        if beta < 0:
            raise ValueError('beta must be positive')
        # Set high buffering to avoid one-line reads
        self.buffering = 64 * 1024 * 1024
        self.n_contents = n_contents
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.contents_file = contents_file
        self.labels_file = labels_file
        self.content_locations =content_locations
        self.rates_file = rates_file
        self.coeffs = []
        with open(rates_file, 'r') as f:
            data = csv.reader(f)
            for co in data:
                first = True
                for coeff in co:
                    if first:
                        self.rate = float(coeff) + float(coeff)*0.1
                        first = False
                    else:
                        self.coeffs.append(float(coeff))
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.contents = []
        contents = []
        with open(self.contents_file, 'r', buffering=self.buffering) as f:
            data = csv.reader(f)
            for content in data:
                for cont in content:
                    contents.append(cont)
        self.labels = []
        labels = []
        with open(self.labels_file, 'r', buffering=self.buffering) as f:
            data = csv.reader(f)
            for label in data:
                for l in label:
                    labels.append(l)
        locations = []
        with open(self.content_locations, 'r', buffering=self.buffering) as loc:
            data = csv.reader(loc)
            for cont_loc in data:
                for c_loc in cont_loc:
                    locations.append(int(c_loc))
        for location in locations:
            self.contents.append(contents[location])
            self.labels.append(labels[location])

        unique_labels = []
        label_counts = Counter()
        for label in self.labels:
            if label not in unique_labels:
                unique_labels.append(label)
            label_counts.update([label])
        self.labels_pdf = {}
        self.labels_weights = {}
        for label in unique_labels:
            self.labels_weights[label] = label_counts[label]
        labels_norm_factor = float(sum(self.labels_weights.values()))
        self.labels_pdf = dict((k, v / labels_norm_factor) for k, v in self.labels_weights.items())

        for number in range(0, len(self.contents)-1):
            self.contents[number] = number
        self.rates_pdf = {}
        for content in self.contents:
            for i in range(0, len(self.contents)-1):
                if self.contents[i] == content:
                    self.rates_pdf[content] = self.coeffs[i]
        contents_weights = {}
        for content in self.rates_pdf:
            contents_weights[content] = self.rates_pdf[content]
        contents_norm_factor = float(sum(contents_weights.values()))
        self.rates_pdf = dict((k, v / contents_norm_factor) for k, v in contents_weights.items())

        self.beta = beta
        if beta != 0:
            degree = nx.degree(topology)
            self.receivers = sorted(self.receivers, key=lambda x:
            degree[iter(topology.edge[x]).next()],
                                    reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers))

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        # if self.first:  # TODO remove this first variable, this is not necessary here - done!
        random.seed(self.seed)
        #    self.first = False
        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            labels = []
            content = None
            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                        labels.append(random_from_pdf(self.labels_pdf))

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(random_from_pdf(self.rates_pdf))  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels.append(random_from_pdf(self.labels_pdf))
                    self.alter = True
                    content = int(random_from_pdf(self.rates_pdf))

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_event
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            # el
            if labels:

                index = int(random_from_pdf(self.rates_pdf))
                datum = self.data[index]
                datum.update(service_type="proc")
                datum.update(labels=labels)

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_event
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            else:
                index = int(random_from_pdf(self.rates_pdf))
                datum = self.data[index]
                datum.update(service_type="proc")
                deadline = self.model.services[content].deadline + t_event
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            #s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1

        print("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()


@register_workload('BURSTY_MORE_LABEL_REQS_DATA')
class BurstyRepoDataAndWorkload(object):
    """This function generates events on the fly, i.e. instead of creating an
    event schedule to be kept in memory, returns an iterator that generates
    events when needed.

    This is useful for running large schedules of events where RAM is limited
    as its memory impact is considerably lower.

    These requests are Poisson-distributed while content popularity is
    Zipf-distributed

    All requests are mapped to receivers uniformly unless a positive *beta*
    parameter is specified.

    If a *beta* parameter is specified, then receivers issue requests at
    different rates. The algorithm used to determine the requests rates for
    each receiver is the following:
     * All receiver are sorted in decreasing order of degree of the PoP they
       are attached to. This assumes that all receivers have degree = 1 and are
       attached to a node with degree > 1
     * Rates are then assigned following a Zipf distribution of coefficient
       beta where nodes with higher-degree PoPs have a higher request rate

    Parameters
    ----------
    topology : fnss.Topology
        The topology to which the workload refers
    n_contents : int
        The number of content object
    alpha : float
        The Zipf alpha parameter
    beta : float, optional
        Parameter indicating
    rate : float, optional
        The mean rate of requests per second
    n_warmup : int, optional
        The number of warmup requests (i.e. requests executed to fill cache but
        not logged)
    n_measured : int, optional
        The number of logged requests after the warmup

    Returns
    -------
    events : iterator
        Iterator of events. Each event is a 2-tuple where the first element is
        the timestamp at which the event occurs and the second element is a
        dictionary of event attributes.

        TODO: Introduce Repo data upload as separate process, mimmicking the request generation and upload!!!!!!!!!!!!!!
    """

    def __init__(self, topology, n_contents, alpha, max_on=0, max_off=0, disrupt_mode=None, disrupt_weights=None,
                 data_gen_dist_mode=None, data_gen_dist=None, data_rate=0, stor_shelf=0, stor_scope=0, n_stor_warmup=0,
                 n_stor_measured=0, beta=0, label_ex=False, alpha_labels=0, rate=100, n_warmup=10 ** 5,
                 n_measured=4 * 10 ** 5, seed=0, n_services=10, topics=None, types=None, max_labels=1, freshness_pers=0,
                 shelf_lives=0, msg_sizes=1000000, **kwargs):
        if types is None:
            types = []
        if alpha < 0:
            raise ValueError('alpha must be positive')

        if alpha_labels < 0:
            raise ValueError('alpha_labels must be positive')

        if beta < 0:
            raise ValueError('beta must be positive')
        self.receivers = [v for v in topology.nodes()
                          if topology.node[v]['stack'][0] == 'receiver']
        self.zipf = TruncatedZipfDist(alpha, n_services - 1, seed)
        self.labels_zipf = TruncatedZipfDist(alpha_labels, len(topics) + len(types), seed)

        self.n_contents = n_contents
        # THIS is where CONTENTS are generated!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # TODO: Associate all below content properties to contents, according to CONFIGURATION required IN FILE, in
        #  contentplacement.py file, registered placement strategies!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.contents = range(0, n_contents)
        self.data = dict()

        for content in self.contents:
            if type(content) is not dict:
                datum = dict()
                datum.update(content=content)
            else:
                datum = content
            datum.update(service_type="proc")
            datum.update(labels=[])
            datum.update(msg_size=msg_sizes)
            datum.update(shelf_life=[])
            datum.update(freshness_per=freshness_pers)
            self.data[content] = datum

        self.labels = dict(topics=topics,
                           types=types)
        self.freshness_pers = freshness_pers
        self.shelf_lives = shelf_lives
        self.sizes = msg_sizes

        self.n_services = n_services
        self.alpha = alpha
        self.alpha_labels = alpha_labels
        self.max_labels = max_labels
        self.label_ex = label_ex
        self.alter = False
        self.rate = rate
        self.n_warmup = n_warmup
        self.n_measured = n_measured
        self.model = None
        self.beta = beta
        self.topology = topology

        # Requests/Data generation disruption settings
        self.max_on = max_on
        self.max_off = max_off
        self.disrupt_mode = disrupt_mode
        self.disrupt_weights = disrupt_weights
        self.receivers_down = {}
        self.last_event = {}
        for receiver in self.receivers:
            self.receivers_down[receiver] = None
            self.last_event[receiver] = None
        self.receivers_downtime = {}
        self.receivers_uptime = {}

        # Continuous data (for storage) generation settings
        self.data_gen_dist_mode = data_gen_dist_mode
        self.data_gen_dist = data_gen_dist
        self.data_rate = data_rate
        self.stor_shelf = stor_shelf
        self.stor_scope = stor_scope
        self.n_stor_warmup = n_stor_warmup
        self.n_stor_measured = n_stor_measured
        self.data_gen_pdf = {}
        if self.data_gen_dist_mode:
            for node in self.data_gen_dist:
                for n in range(0, len(self.receivers)):
                    if self.receivers[n] == node:
                        self.data_gen_pdf[node] = self.receivers[n]

        if beta != 0:
            degree = nx.degree(self.topology)
            self.receivers = sorted(self.receivers, key=lambda x: degree[iter(topology.edge[x]).next()], reverse=True)
            self.receiver_dist = TruncatedZipfDist(beta, len(self.receivers), seed)

        self.seed = seed
        self.first = True

    def __iter__(self):
        req_counter = 0
        t_event = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        if self.first:
            random.seed(self.seed)
            self.first = False
            num_rec_disrupt = random.uniform(0, len(self.receivers) - 1)

        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while req_counter < self.n_warmup + self.n_measured or len(self.model.eventQ) > 0:
            t_event += (random.expovariate(self.rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_event:
                heapq.heappop(self.model.eventQ)
                log = (req_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if req_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.beta == 0:
                receiver = random.choice(self.receivers)
            else:
                receiver = self.receivers[self.receiver_dist.rv() - 1]
            node = receiver

            if self.disrupt_mode == 'RAND':
                count = 0
                for n in self.receivers_down:
                    if n is not None:
                        count += 1
                if num_rec_disrupt >= count:
                    self.receivers_downtime[node] = random.uniform(0, self.max_off)
                    self.receivers_uptime[node] = random.uniform(0, self.max_on)
                    if self.last_event[node] is not None and self.receivers_down[node] is None:
                        self.receivers_down[node] = self.last_event[node] + self.receivers_downtime[node]
                    elif self.receivers_down[node] is not None:
                        if t_event < self.receivers_down[node]:
                            t_event = self.receivers_down[node]
                        elif t_event > self.receivers_down[node] + self.receivers_uptime[node]:
                            self.receivers_down[node] = None
                    self.last_event[node] = t_event

            labels = []
            content = None
            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                    labels_zipf = int(self.labels_zipf.rv())
                    if labels_zipf < len(self.labels['topics']):
                        labels.append(self.labels['topics'][labels_zipf])
                    elif labels_zipf >= len(self.labels['topics']):
                        labels.append(self.labels['types'][labels_zipf - len(self.labels['topics']) - 1])

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels_zipf = int(self.labels_zipf.rv())
                        if labels_zipf < len(self.labels['topics']):
                            labels.append(self.labels['topics'][labels_zipf])
                        elif labels_zipf >= len(self.labels['topics']):
                            labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])])
                    self.alter = True
                    content = int(self.zipf.rv())

            log = (req_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_event
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            # el
            if labels:

                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                datum.update(labels=labels)

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_event
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}
            else:
                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="proc")
                deadline = self.model.services[content].deadline + t_event
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': REQUEST}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            # s = str(t_event) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_event, event)
            req_counter += 1



        # # Continuous data (for storage) generation settings
        # self.data_gen_dist_mode
        # self.data_gen_dist
        # self.data_rate
        # self.stor_shelf
        # self.stor_scope
        # self.n_stor_warmup
        # self.n_stor_measured

        stor_counter = 0
        t_data = 0.0
        flow_id = 0

        # TODO: Associate requests with labels and other, deadline/freshless period/shelf-life requirements,
        #       rather than just contents (could be either or both, depending on restrictions - maybe create
        #       more strategies in that case, if needed.

        if self.first:
            random.seed(self.seed)
            self.first = False
            num_rec_disrupt = random.uniform(0, len(self.receivers) - 1)

        # aFile = open('workload.txt', 'w')
        # aFile.write("# Time\tNodeID\tserviceID\n")
        eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
        while stor_counter < self.n_stor_warmup + self.n_stor_measured or len(self.model.eventQ) > 0:
            t_data += (random.expovariate(self.data_rate))
            eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None
            while eventObj is not None and eventObj.time < t_data:
                heapq.heappop(self.model.eventQ)
                log = (stor_counter >= self.n_warmup)
                event = {'receiver': eventObj.receiver, 'content': eventObj.service, 'labels': eventObj.labels,
                         'log': log, 'node': eventObj.node, 'flow_id': eventObj.flow_id, 'deadline': eventObj.deadline,
                         'rtt_delay': eventObj.rtt_delay, 'status': eventObj.status, 'task': eventObj.task}

                yield (eventObj.time, event)
                eventObj = self.model.eventQ[0] if len(self.model.eventQ) > 0 else None

            if stor_counter >= (self.n_warmup + self.n_measured):
                # skip below if we already sent all the requests
                continue

            if self.data_gen_dist_mode:
                if self.data_gen_dist_mode == "WEIGHTED_DATA_GEN":
                    receiver = random_from_pdf(self.data_gen_pdf)
            else:
                    receiver = random.choice(self.receivers)
            node = receiver

            if self.disrupt_mode == 'RAND':
                count = 0
                for n in self.receivers_down:
                    if n is not None:
                        count += 1
                if num_rec_disrupt >= count:
                    self.receivers_downtime[node] = random.uniform(0, self.max_off)
                    self.receivers_uptime[node] = random.uniform(0, self.max_on)
                    if self.last_data[node] is not None and self.receivers_down[node] is None:
                        self.receivers_down[node] = self.last_data[node] + self.receivers_downtime[node]
                    elif self.receivers_down[node] is not None:
                        if t_data < self.receivers_down[node]:
                            t_data = self.receivers_down[node]
                        elif t_data > self.receivers_down[node] + self.receivers_uptime[node]:
                            self.receivers_down[node] = None
                    self.last_data[node] = t_data

            labels = []
            content = None

            # TODO: INCLUDE stor_scope AND THEN PASS self.stor_scope (workload.stor_scope) TO THE CONTENT PLACEMENT
            #  IMPLEMENTATION IN THE ORCHESTRATOR
            #  Done, but the DATA_SCOPE must be used CAREFULLY, as MAX_REPLICATIONS for content placement
            #  configuration, as well!

            if self.label_ex is True:

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/
                for i in range(0, self.max_labels):
                    labels_zipf = int(self.labels_zipf.rv())
                    if labels_zipf < len(self.labels['topics']):
                        labels.append(self.labels['topics'][labels_zipf])
                    elif labels_zipf >= len(self.labels['topics']):
                        labels.append(self.labels['types'][labels_zipf - len(self.labels['topics']) - 1])

            else:
                if self.alpha_labels == 0 or self.alter is True:
                    content = int(self.zipf.rv())  # TODO: THIS is where the content identifier requests are generated!

                # TODO: Might need to revise this, programming-wise, to account for no content association to selected
                #  label\/\/\/\/\/\/\/\/

                elif self.alter is False:
                    for i in range(0, self.max_labels):
                        labels_zipf = int(self.labels_zipf.rv())
                        if labels_zipf < len(self.labels['topics']):
                            labels.append(self.labels['topics'][labels_zipf])
                        elif labels_zipf >= len(self.labels['topics']):
                            labels.append(self.labels['types'][labels_zipf - len(self.labels['topics'])])
                    self.alter = True
                    content = int(self.zipf.rv())

            log = (stor_counter >= self.n_warmup)
            flow_id += 1
            # TODO: Since services are associated with deadlines based on labels as well now, this should be
            #  accounted for in service placement from now on, as well, the lowest deadline being prioritised
            #  when instantiated (unless a certain  content/service strategy is implemented, maybe).

            # TODO: CHANGE OF PLANS: This would overcomplicate things - just associating labels to "services"!!!!!!!!!!!

            # if content is None:
            #     index = int(self.zipf.rv())
            #     datum = self.data[index]
            #     datum['content'] = ''
            #     datum.update(service_type="non-proc")
            #     datum.update(labels=[])
            #
            #     deadline = self.model.services[index].deadline + t_data
            #     self.model.node_labels[node] = dict()
            #     self.model.node_labels[node].update(request_labels=labels)
            #     for label in labels:
            #         self.model.request_labels_nodes[label] = []
            #         self.model.request_labels_nodes[label].append(node)
            #     datum.update(labels=labels)
            #     event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
            #              'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': STORE}
            # el
            if labels:

                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="non-proc")
                datum.update(labels=labels)
                datum.update(shelf_life=self.stor_shelf)
                datum.update(max_replications=self.stor_scope)

                self.model.node_labels[node] = dict()
                for c in self.data:
                    if all(label in labels for label in self.data[c]['labels']):
                        index = self.data[c]['content']
                deadline = self.model.services[index].deadline + t_data
                self.model.node_labels[node] = dict()
                if node not in self.model.request_labels:
                    self.model.request_labels[node] = Counter()
                self.model.request_labels[node].update(labels)
                for label in labels:
                    if label not in self.model.request_labels_nodes:
                        self.model.request_labels_nodes[label] = Counter()
                    self.model.request_labels_nodes[label].update([node])
                event = {'receiver': receiver, 'content': datum, 'labels': labels, 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': STORE}
            else:
                index = int(self.zipf.rv())
                datum = self.data[index]
                datum.update(service_type="non-proc")
                deadline = self.model.services[content].deadline + t_data
                event = {'receiver': receiver, 'content': datum, 'labels': [], 'log': log, 'node': node,
                         'flow_id': flow_id, 'rtt_delay': 0, 'deadline': deadline, 'status': STORE}

            # NOTE: STOPPED HERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            neighbors = self.topology.neighbors(receiver)
            # s = str(t_data) + "\t" + str(neighbors[0]) + "\t" + str(content) + "\n"
            # aFile.write(s)
            yield (t_data, event)
            stor_counter += 1

        print("End of iteration: len(eventObj): " + repr(len(self.model.eventQ)))
        # aFile.close()
        raise StopIteration()
