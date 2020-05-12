# -*- coding: utf-8 -*-
"""Content placement strategies.

This module contains function to decide the allocation of content objects to
source nodes.
"""
import random
import collections

from fnss.util import random_from_pdf
from icarus.registry import register_content_placement


__all__ = ['uniform_content_placement', 'uniform_repo_content_placement',
           'weighted_content_placement', 'weighted_repo_content_placement']


def apply_content_placement(placement, topology):
    """Apply a placement to a topology

    Parameters
    ----------
    placement : dict of sets
        Set of contents to be assigned to nodes keyed by node identifier
    topology : Topology
        The topology
    """
    for v, contents in placement.items():
        topology.nodes[v]['stack'][1].update(contents = contents)

def apply_service_association(association, data):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for service_type, content in association.items():
        if service_type not in data[content]['service_type']:
            dict(data[content]).update(service_type=service_type)
    return data

def apply_labels_association(association, data):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for label, content in association.items():
        if label not in data[content]['labels']:
            list(dict(data[content])['labels']).append(label)
    return data

def get_sources(topology):
    return [v for v in topology if topology.node[v]['stack'][0] == 'source']

def get_sources_repos(topology):
    try:
        return [v for v in topology if topology.node[v]['stack'][0] == 'source' or
                'source' and 'router' in topology.node[v]['extra-types']]
    except Exception as e:
        err_type = str(type(e)).split("'")[1].split(".")[1]
        if err_type == "KeyError":
            return [v for v in topology if topology.node[v]['stack'][0] == 'source']

@register_content_placement('UNIFORM')
def uniform_content_placement(topology, contents, seed=None):
    """Places content objects to source nodes randomly following a uniform
    distribution.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects
    source_nodes : list
        List of nodes of the topology which are content sources

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    source_nodes = get_sources(topology)
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random.choice(source_nodes)].add(c)
    apply_content_placement(content_placement, topology)

@register_content_placement('UNIFORM_REPO')
def uniform_repo_content_placement(topology, contents, seed=None):
    """Places content objects to source nodes randomly following a uniform
    distribution.

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    source_nodes : list
        List of nodes of the topology which are content sources

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    source_nodes = get_sources_repos(topology)
    content_placement = collections.defaultdict(dict)
    for c in contents:
        choice = random.choice(source_nodes)
        dict1 = {c['content']: c}
        content_placement[choice].update(dict1)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED')
def weighted_content_placement(topology, contents, source_weights, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects

   source_weights : dict
        Dict mapping nodes nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random_from_pdf(source_pdf)].add(c)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED_REPO')
def weighted_repo_content_placement(topology, contents, freshness_per, shelf_life,
                                    msg_size, source_weights, service_weights,
                                    types_weights, topics_weights, max_label_nos, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.
        ADD SERCICE TYPE TO MESSAGE PROPERTIES!

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = dict()
    random.seed(seed)
    for c in contents:
        dict(placed_data[c]).update(content=c)
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    if types_weights is not None:
        types_labels_norm_factor = float(sum(types_weights.values()))
        types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    service_labels_norm_factor = float(sum(service_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    service_labels_pdf = dict((k, v / service_labels_norm_factor) for k, v in service_weights.items())
    service_association = collections.defaultdict(set)
    labels_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    alter = False
    for c in contents:
        for i in range(1, max_label_nos):
            if types_weights is not None and not alter:
                labels_association[random_from_pdf(types_labels_pdf)].add(c)
                alter = True
            elif topics_weights is not None and alter:
                labels_association[random_from_pdf(topics_labels_pdf)].add(c)
                alter = False
        placed_data = apply_labels_association(labels_association, placed_data)
        if freshness_per is not None:
            placed_data[c].update(freshness_per=freshness_per)
        if shelf_life is not None:
            placed_data[c].update(shelf_life=shelf_life)
        service_association[random_from_pdf(service_labels_pdf)].add(c)
        placed_data = apply_service_association(service_association, placed_data)
        placed_data[c].update(msg_size=msg_size)
        placed_data[c]["receiveTime"] = 0
    for d in placed_data:
        content_placement[random_from_pdf(source_pdf)].add(d)
    apply_content_placement(content_placement, topology)
    topology.placed_data = placed_data
