# -*- coding: utf-8 -*-
"""Performance metrics loggers

This module contains all data collectors that record events while simulations
are being executed and compute performance metrics.

Currently implemented data collectors allow users to measure cache hit ratio,
latency, path stretch and link load.

To create a new data collector, it is sufficient to create a new class
inheriting from the `DataCollector` class and override all required methods.

TODO: Create collectors for the different feedback mechanisms, which can then
    be accessed internally (within the simulation, dynamically, by the routing
    and storage strategies, to place contents in the appropriate repos.
    CREATE COLLECTOR METHOD FOR RECORDING ALL INCOMING REQUESTS AND ANOTHER FOR
    RECORDING ALL DATA STORAGE/ENTRIES - REFER TO NETWORK.PY
"""
from __future__ import division
import collections

from icarus.registry import register_data_collector
from icarus.tools import cdf
from icarus.util import Tree, inheritdoc


__all__ = [
    'DataCollector',
    'CollectorProxy',
    'CacheHitRatioCollector',
    'LinkLoadCollector',
    'LatencyCollector',
    'PathStretchCollector',
    'DummyCollector'
           ]


class DataCollector(object):
    """Object collecting notifications about simulation events and measuring
    relevant metrics.
    """

    def __init__(self, view, **params):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            An instance of the network view
        params : keyworded parameters
            Collector parameters
        """
        self.view = view

    def start_session(self, timestamp, receiver, content, labels, flow_id=0, deadline=0):
        """Notifies the collector that a new network session started.

        A session refers to the retrieval of a content from a receiver, from
        the issuing of a content request to the delivery of the content.

        Parameters
        ----------
        timestamp : int
            The timestamp of the event
        receiver : any hashable type
            The receiver node requesting a content
        content : any hashable type
            The content identifier requested by the receiver
        feedback: bool
            *True* if this session and collector uses feedback for the specific content/function,
                    in content placement and performance optimisation
            *False* otherwise
        """
        pass

    def cache_hit(self, node):
        """Reports that the requested content has been served by the cache at
        node *node*.

        Parameters
        ----------
        node : any hashable type
            The node whose cache served the content
        """
        pass

    def cache_miss(self, node):
        """Reports that the cache at node *node* has been looked up for
        requested content but there was a cache miss.

        Parameters
        ----------
        node : any hashable type
            The node whose cache served the content
        """
        pass

    def storage_hit(self, node):
        """Reports that the requested content has been served by the repo storage at
        node *node*.

        Parameters
        ----------
        node : any hashable type
            The node whose cache served the content
        """
        pass

    def storage_miss(self, node):
        """Reports that the repo storage at node *node* has been looked up for
        requested content but there was a repo storage miss.

        Parameters
        ----------
        node : any hashable type
            The node whose cache served the content
        """
        pass

    def server_hit(self, node):
        """Reports that the requested content has been served by the server at
        node *node*.

        Parameters
        ----------
        node : any hashable type
            The server node which served the content
        """
        pass
    
    def replacement_interval_over(self, replacement_interval, timestamp):
        """ Reports the end of a replacement interval for services
        """

        pass

    def execute_service(self, flow_id, service, node, timestamp, is_cloud):
        """ Reports the end of a replacement interval for services
        """

        pass
    
    def reassign_vm(self, node, serviceToReplace, serviceToAdd):
        """ Reports the instantiation of a VM running the service "serviceToAdd", 
            optionally replacing a VM which is running the service serviceToReplace.
        """ 

        pass

    def request_hop(self, u, v, main_path=True):
        """Reports that a request has traversed the link *(u, v)*

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        main_path : bool, optional
            If *True*, indicates that link link is on the main path that will
            lead to hit a content. It is normally used to calculate latency
            correctly in multicast cases. Default value is *True*
        """
        pass

    def content_hop(self, u, v, main_path=True):
        """Reports that a content has traversed the link *(u, v)*

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        main_path : bool, optional
            If *True*, indicates that this link is being traversed by content
            that will be delivered to the receiver. This is needed to
            calculate latency correctly in multicast cases. Default value is
            *True*
        """
        pass

    def end_session(self, success=True, timestamp=0, flow_id=0):
        """Reports that the session is closed, i.e. the content has been
        successfully delivered to the receiver or a failure blocked the
        execution of the request

        Parameters
        ----------
        success : bool, optional
            *True* if the session was completed successfully, *False* otherwise
        """
        pass

    def results(self):
        """Returns the aggregated results measured by the collector.

        Returns
        -------
        results : dict
            Dictionary mapping metric with results.
        """
        pass




# Note: The implementation of CollectorProxy could be improved to avoid having
# to rewrite almost identical methods, for example by playing with __dict__
# attribute. However, it was implemented this way to make it more readable and
# easier to understand.
class CollectorProxy(DataCollector):
    """This class acts as a proxy for all concrete collectors towards the
    network controller.

    An instance of this class registers itself with the network controller and
    it receives notifications for all events. This class is responsible for
    dispatching events of interests to concrete collectors.
    """

    EVENTS = ('start_session', 'end_session', 'cache_hit', 'cache_miss', 'server_hit',
              'request_hop', 'content_hop', 'results', 'replacement_interval_over', 'execute_service'              ,'reassign_vm')

    def __init__(self, view, collectors):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            An instance of the network view
        collector : list of DataCollector
            List of instances of DataCollector that will be notified of events
        """
        self.view = view
        self.collectors = {e: [c for c in collectors if e in type(c).__dict__]
                           for e in self.EVENTS}

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content, labels, flow_id=0, deadline=0):
        for c in self.collectors['start_session']:
            c.start_session(timestamp, receiver, content, labels, flow_id, deadline)

    @inheritdoc(DataCollector)
    def cache_hit(self, node):
        for c in self.collectors['cache_hit']:
            c.cache_hit(node)

    @inheritdoc(DataCollector)
    def cache_miss(self, node):
        for c in self.collectors['cache_miss']:
            c.cache_miss(node)

    @inheritdoc(DataCollector)
    def server_hit(self, node):
        for c in self.collectors['server_hit']:
            c.server_hit(node)

    @inheritdoc(DataCollector)
    def request_hop(self, u, v, main_path=True):
        for c in self.collectors['request_hop']:
            c.request_hop(u, v, main_path)

    @inheritdoc(DataCollector)
    def content_hop(self, u, v, main_path=True):
        for c in self.collectors['content_hop']:
            c.content_hop(u, v, main_path)

    @inheritdoc(DataCollector)
    def replacement_interval_over(self, replacement_interval, timestamp):
        for c in self.collectors['replacement_interval_over']:
            c.replacement_interval_over(replacement_interval, timestamp)

    @inheritdoc(DataCollector)
    def execute_service(self, flow_id, service, node, timestamp, is_cloud):
        for c in self.collectors['execute_service']:
            c.execute_service(flow_id, service, node, timestamp, is_cloud)

    @inheritdoc(DataCollector)
    def reassign_vm(self, node, serviceToReplace, serviceToAdd):
        for c in self.collectors['reassign_vm']:
            c.reassign_vm(node, serviceToReplace, serviceToAdd)

    @inheritdoc(DataCollector)
    def end_session(self, success=True, time=0, flow_id=0):
        for c in self.collectors['end_session']:
            c.end_session(success, time, flow_id)

    @inheritdoc(DataCollector)
    def results(self):
        return Tree(**{c.name: c.results() for c in self.collectors['results']})


@register_data_collector('LINK_LOAD')
class LinkLoadCollector(DataCollector):
    """Data collector measuring the link load
    """

    def __init__(self, view, req_size=150, content_size=1500):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            The network view instance
        req_size : int
            Average size (in bytes) of a request
        content_size : int
            Average size (in byte) of a content
        """
        self.view = view
        self.req_count = collections.defaultdict(int)
        self.cont_count = collections.defaultdict(int)
        if req_size <= 0 or content_size <= 0:
            raise ValueError('req_size and content_size must be positive')
        self.req_size = req_size
        self.content_size = content_size
        self.t_start = -1
        self.t_end = 1

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content):
        if self.t_start < 0:
            self.t_start = timestamp
        self.t_end = timestamp

    @inheritdoc(DataCollector)
    def request_hop(self, u, v, main_path=True):
        self.req_count[(u, v)] += 1

    @inheritdoc(DataCollector)
    def content_hop(self, u, v, main_path=True):
        self.cont_count[(u, v)] += 1

    @inheritdoc(DataCollector)
    def results(self):
        duration = self.t_end - self.t_start
        used_links = set(self.req_count.keys()).union(set(self.cont_count.keys()))
        link_loads = dict((link, (self.req_size * self.req_count[link] +
                                  self.content_size * self.cont_count[link]) / duration)
                          for link in used_links)
        link_loads_int = dict((link, load)
                              for link, load in link_loads.items()
                              if self.view.link_type(*link) == 'internal')
        link_loads_ext = dict((link, load)
                              for link, load in link_loads.items()
                              if self.view.link_type(*link) == 'external')
        mean_load_int = sum(link_loads_int.values()) / len(link_loads_int) \
                        if len(link_loads_int) > 0 else 0
        mean_load_ext = sum(link_loads_ext.values()) / len(link_loads_ext) \
                        if len(link_loads_ext) > 0 else 0
        return Tree({'MEAN_INTERNAL':     mean_load_int,
                     'MEAN_EXTERNAL':     mean_load_ext,
                     'PER_LINK_INTERNAL': link_loads_int,
                     'PER_LINK_EXTERNAL': link_loads_ext})


@register_data_collector('LATENCY')
class LatencyCollector(DataCollector):
    """Data collector measuring latency, i.e. the delay taken to delivery a
    content.
    """

    def __init__(self, view, cdf=False):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            The network view instance
        cdf : bool, optional
            If *True*, also collects a cdf of the latency
        """
        self.cdf = cdf
        self.view = view
        self.sess_count = 0
        self.interval_sess_count = 0
        self.latency_interval = 0.0
        self.deadline_metric_interval = 0.0
        self.n_satisfied = 0.0 # number of satisfied requests
        self.n_satisfied_interval = 0.0
        self.n_sat_cloud_interval = 0
        self.n_instantiations_interval = 0

        # Cache and storage
        self.cache_hits = 0
        self.storage_hits = 0
        self.cache_misses = 0
        self.storage_misses = 0
        self.serv_hits = 0

        self.flow_start = {} # flow_id to start time
        self.flow_cloud = {} # True if flow reched cloud
        self.flow_service = {} # flow id to service
        self.flow_deadline = {} # flow id to deadline
        self.flow_labels = {} # flow id to service-associated labels
        self.flow_feedback = {} # flow id to service-associated labels
        self.service_requests = {} #number of requests per service
        self.service_satisfied = {} #number of satisfied requests per service

        # Time series for various metrics
        self.satrate_times = {}
        self.latency_times = {}
        self.idle_times = {}
        self.node_idle_times = {}
        self.deadline_metric_times = {}
        self.cloud_sat_times = {}
        self.instantiations_times = {}

        if cdf:
            self.latency_data = collections.deque()
        self.css = self.view.service_nodes()
        #self.n_services = self.css.items()[0][1].numOfVMs
    
    @inheritdoc(DataCollector)
    def execute_service(self, flow_id, service, node, timestamp, is_cloud):
        if is_cloud:
            self.flow_cloud[flow_id] = True
        else:
            self.flow_cloud[flow_id] = False

    @inheritdoc(DataCollector)
    def reassign_vm(self, node, serviceToReplace, serviceToAdd): 
        self.n_instantiations_interval += 1

    @inheritdoc(DataCollector)
    def replacement_interval_over(self, replacement_interval, timestamp):
        if self.interval_sess_count == 0:
            self.satrate_times[timestamp] = 0.0
        else:
            self.satrate_times[timestamp] = self.n_satisfied_interval/self.interval_sess_count
        print ("Number of requests in interval: " + repr(self.interval_sess_count))

        self.instantiations_times[timestamp] = self.n_instantiations_interval
        self.n_instantiations_interval = 0
        
        total_idle_time = 0.0
        total_cores = 0 # total number of cores in the network
        for node, cs in self.css.items():
            if cs.is_cloud:
                continue
            
            idle_time = cs.getIdleTime(timestamp)
            idle_time /= cs.numOfCores
            total_idle_time += idle_time
           
            total_cores += cs.numOfCores 

            if node not in self.node_idle_times.keys():
                self.node_idle_times[node] = []

            self.node_idle_times[node].append(idle_time) 

        #self.idle_times[timestamp] = total_idle_time
        self.idle_times[timestamp] = total_idle_time / (total_cores*replacement_interval)
        if self.n_satisfied_interval == 0:
            self.latency_times[timestamp] = 0.0
            self.deadline_metric_times[timestamp] = 0.0
            self.cloud_sat_times[timestamp] = 0.0
        else:
            self.latency_times[timestamp] = self.latency_interval/self.n_satisfied_interval
            self.deadline_metric_times[timestamp] = self.deadline_metric_interval/self.n_satisfied_interval
            self.cloud_sat_times[timestamp] = (1.0*self.n_sat_cloud_interval)/self.n_satisfied_interval
        #self.per_service_idle_times[timestamp] = [avg_idle_times[x]/replacement_interval for x in range(0, self.n_services)]

        # Initialise interval counts
        self.n_sat_cloud_interval = 0
        self.interval_sess_count = 0
        self.n_satisfied_interval = 0
        self.latency_interval = 0.0
        self.deadline_metric_interval = 0.0

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content, labels, flow_id=0, deadline=0):
        self.sess_count += 1
        self.sess_latency = 0.0
        self.flow_start[flow_id] = timestamp
        self.flow_deadline[flow_id] = deadline
        self.flow_service[flow_id] = content
        self.flow_cloud[flow_id] = False
        self.interval_sess_count += 1
        self.flow_labels[flow_id] = labels

    @inheritdoc(DataCollector)
    def cache_hit(self, node):
        self.cache_hits += 1

    @inheritdoc(DataCollector)
    def storage_hit(self, node):
        self.storage_hits += 1

    @inheritdoc(DataCollector)
    def cache_miss(self, node):
        self.session['cache_misses'].append(node)

    @inheritdoc(DataCollector)
    def storage_miss(self, node):
        self.session['storage_misses'].append(node)

    @inheritdoc(DataCollector)
    def request_hop(self, u, v, main_path=True):
        if main_path:
            self.sess_latency += self.view.link_delay(u, v)

    @inheritdoc(DataCollector)
    def content_hop(self, u, v, main_path=True):
        if main_path:
            self.sess_latency += self.view.link_delay(u, v)

    @inheritdoc(DataCollector)
    def end_session(self, success=True, timestamp=0, flow_id=0):
        sat = False
        if not success:
            return
        if self.cdf:
            self.latency_data.append(self.sess_latency)

        if self.flow_deadline[flow_id] >= timestamp:
            # Request is satisfied
            if self.flow_cloud[flow_id]:
                self.n_sat_cloud_interval += 1
            self.n_satisfied += 1
            print "Request satisfied"
            self.n_satisfied_interval += 1
            sat = True 
            self.latency_interval += timestamp - self.flow_start[flow_id]
            self.deadline_metric_interval += self.flow_deadline[flow_id] - timestamp

        service = self.flow_service[flow_id]
        if service not in self.service_requests.keys():
            self.service_requests[service['content']] = 1
            self.service_satisfied[service['content']] = 0
        else:
            self.service_requests[service['content']] += 1

        if sat:
            if service in self.service_satisfied.keys():
                self.service_satisfied[service['content']] += 1
            else:
                self.service_satisfied[service['content']] = 1

        del self.flow_deadline[flow_id]
        del self.flow_start[flow_id]
        del self.flow_service[flow_id]
        del self.flow_cloud[flow_id]

    @inheritdoc(DataCollector)
    def results(self):
        if self.view.model.strategy == 'HYBRID':
            res = open("/home/chrisys/Icarus-repos/IIcarusEdgeSim/examples/repos-mgmt/hybrid.txt", 'a')
        elif self.view.model.strategy == 'HYBRIDS_REPO_APP':
            res = open("/home/chrisys/Icarus-repos/IIcarusEdgeSim/examples/repos-mgmt/hybrid_repo.txt", 'a')
        elif self.view.model.strategy == 'HYBRIDS_PRO_REPO_APP':
            res = open("/home/chrisys/Icarus-repos/IIcarusEdgeSim/examples/repos-mgmt/hybrid_pro_repo.txt", 'a')
        elif self.view.model.strategy == 'HYBRIDS_RE_REPO_APP':
            res = open("/home/chrisys/Icarus-repos/IIcarusEdgeSim/examples/repos-mgmt/hybrid_re_repo.txt", 'a')
        elif self.view.model.strategy == 'HYBRIDS_SPEC_REPO_APP':
            res = open("/home/chrisys/Icarus-repos/IcarusEdgeSim/examples/repos-mgmt/hybrid_spec_repo.txt", 'a')
        if self.cdf:
            self.results['CDF'] = cdf(self.latency_data)
        results = Tree({'SATISFACTION' : 1.0*self.n_satisfied/self.sess_count})

        per_service_sats = {}
        res.write(str(100*self.n_satisfied/self.sess_count) + " " + str(self.n_satisfied) + " " + str(self.sess_count) + ": \n")
        for service in self.service_requests.keys():
            per_service_sats[service] = 1.0*self.service_satisfied[service]/self.service_requests[service]
            res.write(str(100*self.service_satisfied[service]/self.service_requests[service]) + ", ")
        res.write("\n")
        results['PER_SERVICE_SATISFACTION'] = per_service_sats
        results['PER_SERVICE_REQUESTS'] = self.service_requests
        results['PER_SERVICE_SAT_REQUESTS'] = self.service_satisfied
        results['SAT_TIMES'] = self.satrate_times
        results['IDLE_TIMES'] = self.idle_times 
        results['NODE_IDLE_TIMES'] = self.node_idle_times
        results['LATENCY'] = self.latency_times
        results['DEADLINE_METRIC'] = self.deadline_metric_times
        results['CLOUD_SAT_TIMES'] = self.cloud_sat_times
        results['INSTANTIATION_OVERHEAD'] = self.instantiations_times

        print "Printing Sat. rate times:"
        for key in sorted(self.satrate_times):
            print (repr(key) + " " + repr(self.satrate_times[key]))
        
        print "Printing Idle times:"
        for key in sorted(self.idle_times):
            print (repr(key) + " " + repr(self.idle_times[key]))
        #results['VMS_PER_SERVICE'] = self.vms_per_service
        res.close()
        
        return results

@register_data_collector('CACHE_HIT_RATIO')
class CacheHitRatioCollector(DataCollector):
    """Collector measuring the cache hit ratio, i.e. the portion of content
    requests served by a cache.
    """

    def __init__(self, view, off_path_hits=False, per_node=True, content_hits=False):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            The NetworkView instance
        off_path_hits : bool, optional
            If *True* also records cache hits from caches not on located on the
            shortest path. This metric may be relevant only for some strategies
        content_hits : bool, optional
            If *True* also records cache hits per content instead of just
            globally
        """
        self.view = view
        self.off_path_hits = off_path_hits
        self.per_node = per_node
        self.cont_hits = content_hits
        self.sess_count = 0
        self.cache_hits = 0
        self.serv_hits = 0
        if off_path_hits:
            self.off_path_hit_count = 0
        if per_node:
            self.per_node_cache_hits = collections.defaultdict(int)
            self.per_node_server_hits = collections.defaultdict(int)
        if content_hits:
            self.curr_cont = None
            self.cont_cache_hits = collections.defaultdict(int)
            self.cont_serv_hits = collections.defaultdict(int)

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content):
        self.sess_count += 1
        if self.off_path_hits:
            source = self.view.content_source(content)
            self.curr_path = self.view.shortest_path(receiver, source)
        if self.cont_hits:
            self.curr_cont = content

    @inheritdoc(DataCollector)
    def cache_hit(self, node):
        self.cache_hits += 1
        if self.off_path_hits and node not in self.curr_path:
            self.off_path_hit_count += 1
        if self.cont_hits:
            self.cont_cache_hits[self.curr_cont] += 1
        if self.per_node:
            self.per_node_cache_hits[node] += 1

    @inheritdoc(DataCollector)
    def server_hit(self, node):
        self.serv_hits += 1
        if self.cont_hits:
            self.cont_serv_hits[self.curr_cont] += 1
        if self.per_node:
            self.per_node_server_hits[node] += 1

    @inheritdoc(DataCollector)
    def results(self):
        n_sess = self.cache_hits + self.serv_hits
        hit_ratio = self.cache_hits / n_sess
        results = Tree(**{'MEAN': hit_ratio})
        if self.off_path_hits:
            results['MEAN_OFF_PATH'] = self.off_path_hit_count / n_sess
            results['MEAN_ON_PATH'] = results['MEAN'] - results['MEAN_OFF_PATH']
        if self.cont_hits:
            cont_set = set(list(self.cont_cache_hits.keys()) + list(self.cont_serv_hits.keys()))
            cont_hits = dict((i, (self.cont_cache_hits[i] / (self.cont_cache_hits[i] + self.cont_serv_hits[i])))
                            for i in cont_set)
            results['PER_CONTENT'] = cont_hits
        if self.per_node:
            for v in self.per_node_cache_hits:
                self.per_node_cache_hits[v] /= n_sess
            for v in self.per_node_server_hits:
                self.per_node_server_hits[v] /= n_sess
            results['PER_NODE_CACHE_HIT_RATIO'] = self.per_node_cache_hits
            results['PER_NODE_SERVER_HIT_RATIO'] = self.per_node_server_hits
        return results


@register_data_collector('PATH_STRETCH')
class PathStretchCollector(DataCollector):
    """Collector measuring the path stretch, i.e. the ratio between the actual
    path length and the shortest path length.
    """

    def __init__(self, view, cdf=False):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            The network view instance
        cdf : bool, optional
            If *True*, also collects a cdf of the path stretch
        """
        self.view = view
        self.cdf = cdf
        self.req_path_len = collections.defaultdict(int)
        self.cont_path_len = collections.defaultdict(int)
        self.sess_count = 0
        self.mean_req_stretch = 0.0
        self.mean_cont_stretch = 0.0
        self.mean_stretch = 0.0
        if self.cdf:
            self.req_stretch_data = collections.deque()
            self.cont_stretch_data = collections.deque()
            self.stretch_data = collections.deque()

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content):
        self.receiver = receiver
        self.source = self.view.content_source(content)
        self.req_path_len = 0
        self.cont_path_len = 0
        self.sess_count += 1

    @inheritdoc(DataCollector)
    def request_hop(self, u, v, main_path=True):
        self.req_path_len += 1

    @inheritdoc(DataCollector)
    def content_hop(self, u, v, main_path=True):
        self.cont_path_len += 1

    @inheritdoc(DataCollector)
    def end_session(self, success=True):
        if not success:
            return
        req_sp_len = len(self.view.shortest_path(self.receiver, self.source))
        cont_sp_len = len(self.view.shortest_path(self.source, self.receiver))
        req_stretch = self.req_path_len / req_sp_len
        cont_stretch = self.cont_path_len / cont_sp_len
        stretch = (self.req_path_len + self.cont_path_len) / (req_sp_len + cont_sp_len)
        self.mean_req_stretch += req_stretch
        self.mean_cont_stretch += cont_stretch
        self.mean_stretch += stretch
        if self.cdf:
            self.req_stretch_data.append(req_stretch)
            self.cont_stretch_data.append(cont_stretch)
            self.stretch_data.append(stretch)

    @inheritdoc(DataCollector)
    def results(self):
        results = Tree({'MEAN': self.mean_stretch / self.sess_count,
                        'MEAN_REQUEST': self.mean_req_stretch / self.sess_count,
                        'MEAN_CONTENT': self.mean_cont_stretch / self.sess_count})
        if self.cdf:
            results['CDF'] = cdf(self.stretch_data)
            results['CDF_REQUEST'] = cdf(self.req_stretch_data)
            results['CDF_CONTENT'] = cdf(self.cont_stretch_data)
        return results


@register_data_collector('DUMMY')
class DummyCollector(DataCollector):
    """Dummy collector to be used for test cases only."""

    def __init__(self, view):
        """Constructor

        Parameters
        ----------
        view : NetworkView
            The network view instance
        output : stream
            Stream on which debug collector writes
        """
        self.view = view

    @inheritdoc(DataCollector)
    def start_session(self, timestamp, receiver, content):
        self.session = dict(timestamp=timestamp, receiver=receiver,
                            content=content, cache_misses=[],
                            request_hops=[], content_hops=[])

    @inheritdoc(DataCollector)
    def cache_hit(self, node):
        self.session['serving_node'] = node

    @inheritdoc(DataCollector)
    def cache_miss(self, node):
        self.session['cache_misses'].append(node)

    @inheritdoc(DataCollector)
    def server_hit(self, node):
        self.session['serving_node'] = node

    @inheritdoc(DataCollector)
    def request_hop(self, u, v, main_path=True):
        self.session['request_hops'].append((u, v))

    @inheritdoc(DataCollector)
    def content_hop(self, u, v, main_path=True):
        self.session['content_hops'].append((u, v))

    @inheritdoc(DataCollector)
    def end_session(self, success=True):
        self.session['success'] = success

    def session_summary(self):
        """Return a summary of latest session

        Returns
        -------
        session : dict
            Summary of session
        """
        return self.session
