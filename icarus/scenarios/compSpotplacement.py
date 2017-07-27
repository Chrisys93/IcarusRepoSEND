# -*- coding: utf-8 -*-
"""Implements computational spot placement strategies
"""

from __future__ import division
import networkx as nx

from icarus.util import iround
from icarus.registry import register_computation_placement

__all__ = [
        'uniform_computation_placement',
        'central_computation_placement'
          ]

@register_computation_placement('CENTRALITY')
def central_computation_placement(topology, computation_budget, service_budget, **kwargs):
    """Places computation budget proportionally to the betweenness centrality of the
    node.
    
    Parameters
    ----------
    topology : Topology
        The topology object
    computation_budget : int
        The cumulative computation budget in terms of the number of VMs
    """
    betw = nx.betweenness_centrality(topology)
    root = [v for v in topology.graph['icr_candidates']
            if topology.node[v]['depth'] == 0][0]
    total_betw = sum(betw.values()) - betw[root]
    icr_candidates = topology.graph['icr_candidates']
    for v in icr_candidates:
        if v == root:
            continue
        topology.node[v]['stack'][1]['computation_size'] = iround(computation_budget*betw[v]/total_betw)
        topology.node[v]['stack'][1]['service_size'] = iround(service_budget*betw[v]/total_betw)
        
    topology.node[root]['stack'][1]['computation_size'] = -1 #TODO have a parameter here
    topology.node[root]['stack'][1]['service_size'] = -1 

@register_computation_placement('UNIFORM')
def uniform_computation_placement(topology, computation_budget, service_budget, **kwargs):
    """Places computation budget uniformly across cache nodes.
    
    Parameters
    ----------
    topology : Topology
        The topology object
    computation_budget : int
        The cumulative computation budget in terms of the number of VMs
    """

    icr_candidates = topology.graph['icr_candidates']
    print("Computation budget: " + repr(computation_budget))
    print("Service budget: " + repr(service_budget))
    cache_size = iround(computation_budget/(len(icr_candidates)-1))
    service_size = iround(service_budget/(len(icr_candidates)-1))
    root = [v for v in icr_candidates if topology.node[v]['depth'] == 0][0]
    for v in icr_candidates:
        if v == root:
            topology.node[v]['stack'][1]['service_size'] = -1
            topology.node[v]['stack'][1]['computation_size'] = -1
            topology.node[v]['stack'][1]['cache_size'] = service_budget
        else:        
            topology.node[v]['stack'][1]['service_size'] = service_size
            topology.node[v]['stack'][1]['computation_size'] = cache_size
            topology.node[v]['stack'][1]['cache_size'] = service_size
