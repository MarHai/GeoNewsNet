import os
import networkx
from decimal import Decimal
from time import time
from datetime import datetime
from collections import Counter
from setup import get_config, get_engine, get_database
from database import Outlet, Scrape, Link
import warnings


class GephiCreator:
    def __init__(self, db):
        self._db = db
        self._graph = networkx.DiGraph()
        self._nodes = []
        self._links = []
        self._edges = None

    def _count_internal_links(self, scrape):
        return self._db.query(Link.url_target).filter(
            Link.is_internal,
            Link.scrape_origin == scrape
        ).group_by(Link.url_target).count()

    def _add_single_outlet(self, outlet):
        data_from_outlet = {}
        for key, value in vars(outlet).items():
            if not key.startswith('_'):
                # Gephi cannot handle Decimal objects, so we force it to string
                # data_from_outlet[key] = str(value) if isinstance(value, Decimal) else value
                data_from_outlet[key] = str(value)
        data_from_outlet['n_unique_internal'] = self._count_internal_links(outlet.scrape)
        if data_from_outlet['n_unique_internal'] == 0:
            warnings.warn('Host %s does not have any internal links, which affects link-ratio calculation' % outlet.fld)
        self._graph.add_node(outlet.fld, **data_from_outlet)
        self._nodes.append(outlet.fld)

    def add_outlets(self, outlets):
        for outlet in outlets:
            self._add_single_outlet(outlet)

    @staticmethod
    def get_link_name(link):
        return '%s -> %s' % (link.fld_origin, link.fld_target)

    @staticmethod
    def get_origin_from_link_name(link_name):
        return link_name.split(' -> ')[0]

    @staticmethod
    def get_target_from_link_name(link_name):
        return link_name.split(' -> ')[1]

    def _add_single_link(self, link):
        if link.fld_origin in self._nodes and link.fld_target in self._nodes and not link.is_internal:
            self._edges[GephiCreator.get_link_name(link)] += 1

    def add_links(self, links):
        self._edges = Counter()
        for link in links:
            self._add_single_link(link)
        for link_name, link_weight in self._edges.most_common():
            link_origin = GephiCreator.get_origin_from_link_name(link_name)
            link_target = GephiCreator.get_target_from_link_name(link_name)
            origin_internal = self._graph.node[link_origin]['n_unique_internal']
            if origin_internal == 0:
                warnings.warn('Skipping link %s due to invalid external-internal link-ratio calculation' % link_name)
            elif link_weight > 0:
                self._graph.add_edge(
                    link_origin,
                    link_target,
                    weight=link_weight,
                    external_internal_ratio=link_weight/origin_internal,
                    Label=link_name
                )

    def count_links(self):
        return self._graph.number_of_edges()

    def count_outlets(self):
        return self._graph.number_of_nodes()

    def get_outlets(self):
        return self._nodes

    def write_gexf(self, file):
        networkx.write_gexf(self._graph, file)


if __name__ == '__main__':
    t0 = time()

    print('GeoNewsNet v2')
    print('https://github.com/MarHai/GeoNewsNet')
    print('(c) 2019 by Mario Haim <mario@haim.it>')
    print('---------')

    config = get_config()
    db_engine = get_engine(config)
    db = get_database(db_engine)
    print('---------')

    print('Setting up the Gephi chart')
    chart = GephiCreator(db)

    chart.add_outlets(db.query(Outlet).filter(Outlet.scrape_uid.isnot(None)).all())
    print('- %d outlets added to the chart' % chart.count_outlets())

    chart.add_links(db.query(Link).filter(
        Link.scrape_target_uid.isnot(None),
        Link.scrape_origin_uid == Scrape.uid,
        Scrape.status_code == 200,
        Link.is_internal.is_(False)
    ).all())
    print('- %d links added to the chart' % chart.count_links())
    print('---------')

    print('Storing Gephi chart')
    directory = 'graph_files/'
    if not os.path.exists(directory):
        os.makedirs(directory)
        print('- directory %s for resulting charts created' % directory)
    filename = 'chart_%s.gexf' % datetime.now().strftime('%Y-%m-%d_%H-%M')

    print('- attempting to write nodes to %s%s' % (directory, filename.replace('.gexf', '.txt')))
    with open(directory + filename.replace('.gexf', '.txt'), 'w') as f:
        for item in chart.get_outlets():
            f.write('%s\n' % item)

    print('- attempting to finally create the chart file as %s%s' % (directory, filename))
    chart.write_gexf(directory + filename)

    print('---------')
    print('Done in %.2f seconds' % (time() - t0))
