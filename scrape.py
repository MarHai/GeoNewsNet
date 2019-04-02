from time import time
from setup import get_config, get_engine, get_database, get_browser_header, send_email
import threading
from queue import Queue
from database import Outlet, Scrape, Link, ScrapeError
from sqlalchemy import or_, func


class Scraper(threading.Thread):
    def __init__(self, queue, config, db_engine):
        threading.Thread.__init__(self)
        self._queue = queue
        self._config = config
        self._logfile = None
        # to be thread-safe, we use a fresh scoped session, which gets initiated here
        self._db = get_database(db_engine)

    def run(self):
        self._logfile = log('  - worker #%d set up' % threading.get_ident())
        while True:
            content = self._queue.get()
            if isinstance(content, str) and content == 'quit':
                self._db.close()
                log('  - worker #%d hereby resigns from his/her duties' % threading.get_ident(), self._logfile)
                break
            elif isinstance(content, Outlet):
                scrape_tmp = self.scrape(content.url)
                if scrape_tmp:
                    # re-query Scrape for thread safety
                    outlet_tmp = self._db.query(Outlet).filter(Outlet.uid == content.uid).one()
                    outlet_tmp.scrape = scrape_tmp
                    self._db.commit()
            elif isinstance(content, Link):
                self.scrape(content.url_target)

    def scrape(self, url):
        """Requests url and extracts links. A Scrape and several 1:n-linked Link objects are created.
        For every Link, existent target Scrape objects are located and incorporated.
        A link is considered "internal" if the first-level domains of origin and target are equal.
        For the new Scrape, existent Link objects targeting this Scrape are located and updated.
        Returns the new Scrape object or False if an error occured.
        """
        try:
            response = Scrape.request(url, get_browser_header(self._config))
            response_url = Link.sanitize_url(response.url)
            if response_url:
                scrape = Scrape(
                    url_started=url,
                    url_finished=response_url,
                    seconds_elapsed=response.elapsed.total_seconds(),
                    status_code=response.status_code
                )
                links = Scrape.extract(response.text, response_url,
                                       self._config.get('Scraper', 'parser', fallback='lxml'))
                for target in links:
                    fld_origin = Link.extract_fld(response.url)
                    fld_target = Link.extract_fld(target)
                    link = Link(
                        url_origin=response_url,
                        fld_origin=fld_origin,
                        url_target=target,
                        fld_target=fld_target,
                        is_internal=(fld_origin == fld_target)
                    )
                    scrape_existent = self._db.query(Scrape).filter(
                        or_(Scrape.url_started == target, Scrape.url_finished == target),
                        Scrape.status_code == 200
                    ).order_by(Scrape.created).first()
                    if scrape_existent is not None:
                        link.scrape_target = scrape_existent
                    scrape.links_outgoing.append(link)
                self._db.add(scrape)
                links_existent = self._db.query(Link).filter(
                    or_(Link.url_target == scrape.url_started, Link.url_target == scrape.url_finished)
                ).all()
                for link_existent in links_existent:
                    link_existent.scrape_target = scrape
                self._db.commit()
                return scrape
        except ScrapeError as e:
            self._db.add(Scrape(
                url_started=url,
                url_finished=e.response.url,
                seconds_elapsed=e.response.elapsed.total_seconds(),
                status_code=e.response.status_code
            ))
            links_existent = self._db.query(Link).filter(
                or_(Link.url_target == url, Link.url_target == e.response.url)
            ).all()
            for link_existent in links_existent:
                link_existent.increase_errors()
            self._db.commit()
        return False


def recursively_add_links_to_queue(queue, current_level, links_from_current_level, max_depth):
    links_actually_added_to_queue = 0
    for link in links_from_current_level:
        if link.scrape_target is None or link.scrape_target.status_code != 200:
            queue.put(link)
            links_actually_added_to_queue = links_actually_added_to_queue + 1
        else:
            if current_level < max_depth:
                links = db.query(Link).filter(Link.scrape_origin_uid == link.scrape_target_uid).all()
                recursively_add_links_to_queue(queue, current_level + 1, links, max_depth)
    if links_actually_added_to_queue > 0:
        log('  - added %d not-yet-visited out of %d potential link targets on level %d to the list' %
            (links_actually_added_to_queue, len(links_from_current_level), current_level))


def log(msg, file=None):
    if file is None:
        filename = 'geonewsnet.log'
        file = open(filename, 'a')
        log('%s opened as logfile' % filename, file)
    file.write(msg + '\n')
    print(msg)
    return file


if __name__ == '__main__':
    t0 = time()

    logfile = log('GeoNewsNet v2')
    log('https://github.com/MarHai/GeoNewsNet', logfile)
    log('(c) 2019 by Mario Haim <mario@haim.it>', logfile)
    log('---------', logfile)

    config = get_config()
    db_engine = get_engine(config)
    db = get_database(db_engine)
    queue = Queue()
    threads = []
    log('---------', logfile)

    workers = int(config.get('Scraper', 'threads', fallback=4))
    max_depth = int(config.get('Scraper', 'depth', fallback=1))

    for round in range(max_depth+1):
        log(':: Round %d of %d' % (round+1, max_depth+1), logfile)

        log('  Creating %d parallel scrapers' % workers, logfile)
        for i in range(workers):
            worker = Scraper(queue, config, db_engine)
            worker.start()
            threads.append(worker)

        log('  Collecting all URLs to be scraped', logfile)
        outlets = db.query(Outlet).filter(Outlet.scrape_uid.is_(None)).all()
        for outlet in outlets:
            queue.put(outlet)
        if len(outlets) > 0:
            log('  - added %d outlets (nodes) to the list' % len(outlets), logfile)

        # for all Outlet-related Scrape objects (level=1), start the recursive process for all Link objects (level=2)
        if max_depth > 1:
            links = db.query(Link).filter(
                Link.scrape_origin_uid == Scrape.uid,
                Scrape.uid == Outlet.scrape_uid,
                Outlet.scrape_uid.isnot(None)
            ).all()
            recursively_add_links_to_queue(queue, 2, links, max_depth)

        for worker in threads:
            queue.put('quit')
        for worker in threads:
            worker.join()
    log('---------', logfile)

    log('Everything done, here are some descriptive statistics:', logfile)
    scrape_total = db.query(func.count(Scrape.uid)).one()[0]
    scrape_successful = db.query(func.count(Scrape.uid)).filter(Scrape.status_code == 200).one()[0]
    log('- %d websites scraped, %d of which (%d%%) were successful (i.e., status code 200)' %
        (scrape_total, scrape_successful, (0 if scrape_total == 0 else 100*scrape_successful/scrape_total)),
        logfile)
    scrape_host_result = db.query(func.count(Link.uid)).group_by(Link.fld_origin)
    if scrape_host_result.count() == 0:
        log('- no hosts scraped', logfile)
    else:
        scrape_min_documents = min(count[0] for count in scrape_host_result.all())
        scrape_max_documents = max(count[0] for count in scrape_host_result.all())
        log('- %d hosts scraped, containing between %d and %d documents' %
            (scrape_host_result.count(), scrape_min_documents, scrape_max_documents),
            logfile)
    try:
        links_internal = db.query(func.count(Link.uid)).filter(Link.is_internal).one()[0]
        links_external = db.query(func.count(Link.uid)).filter(Link.is_internal.is_(False)).one()[0]
    except:
        links_internal = 0
        links_external = 0
    links_total = links_internal + links_external
    log('- %d links collected, %d of which are external (%d%%)' %
        (links_total, links_external, (0 if links_total == 0 else 100*links_external/links_total)),
        logfile)
    links_outlet = db.query(func.count(Link.uid)).outerjoin(Outlet, Link.scrape_target_uid == Outlet.scrape_uid).filter(
        Link.is_internal.is_(False),
        Link.scrape_target_uid.isnot(None)
    ).one()[0]
    log('- %d external links (%d%% out of %d external links) link to pre-configured outlets' %
        (links_outlet, (0 if links_external == 0 else 100*links_outlet/links_external), links_external),
        logfile)
    log('---------', logfile)

    timediff = time() - t0
    log('Done in %.2f seconds' % timediff, logfile)
    send_email(config,
               '[GeoNewsNet] Scrape Process Finished',
               ('Hi,\n\nthe running scrape process has just finished after %.2f seconds.\n' +
                'Please find the full logfile attached.\n\n' +
                'Thanks for using our tools,\nMario\n') % timediff,
               [logfile.name])
