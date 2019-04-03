from time import time
from setup import get_config, get_engine, get_database, get_browser_header, send_email
import threading
from queue import Queue
from database import Outlet, Scrape, Link, ScrapeError
from sqlalchemy import or_, func
from statistics import mean, stdev
import sys
import traceback


class Scraper(threading.Thread):
    def __init__(self, queue, config, db_engine):
        threading.Thread.__init__(self)
        self._queue = queue
        self._config = config
        # to be thread-safe, we use a fresh scoped session, which gets initiated here
        self._db = get_database(db_engine)

    def run(self):
        log('Worker set up', str(threading.get_ident()))
        while True:
            content = self._queue.get()
            if isinstance(content, str) and content == 'quit':
                self._db.close()
                log('Worker resigns from duties', str(threading.get_ident()))
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
        except:
            error = sys.exc_info()
            if error is not None and error[0] is not None:
                log('Error Occurred with %s' % url, ('%s \n\n %s' % (str(error[0]), traceback.format_exc())), True)
            else:
                log('Error Occurred with %s' % url, traceback.format_exc(), True)
        return False


def recursively_add_links_to_queue(queue, current_level, links_from_current_level, max_depth):
    links_actually_added_to_queue = 0
    for link in links_from_current_level:
        if link.scrape_target is None:
            # there is currently no target scrape set for this link
            scrape_existent = db.query(Scrape).filter(
                or_(Scrape.url_started == link.url_target, Scrape.url_finished == link.url_target),
                Scrape.status_code == 200
            ).order_by(Scrape.created).first()
            if scrape_existent is not None:
                # target scrape found, updating link entry (no actual scraping takes place)
                link = db.query(Link).filter(Link.uid == link.uid).one()
                link.scrape_target_uid = scrape_existent.uid
                db.commit()
                if current_level < max_depth:
                    links_actually_added_to_queue += recursively_add_links_to_queue(
                        queue,
                        current_level + 1,
                        db.query(Link).filter(Link.scrape_origin_uid == link.scrape_target_uid).all(),
                        max_depth
                    )
            else:
                # due to multi-threading, we double-checked, but there is still no target scrape found
                queue.put(link)
                links_actually_added_to_queue += 1
        elif link.scrape_target.status_code != 200:
            # target scrape already exists but was not successful (new scrape initiated)
            queue.put(link)
            links_actually_added_to_queue += 1
        else:
            # target scrape found (no actual scraping takes place)
            if current_level < max_depth:
                links_actually_added_to_queue += recursively_add_links_to_queue(
                    queue,
                    current_level + 1,
                    db.query(Link).filter(Link.scrape_origin_uid == link.scrape_target_uid).all(),
                    max_depth
                )
    return links_actually_added_to_queue


def log(gist, msg, very_important_msg=False):
    print(('%s: %s' % (gist, msg)) if len(msg) < 80 else gist)
    if very_important_msg:
        send_email(config,
                   '[GeoNewsNet] %s' % gist,
                   'Hi there!\n\n%s\n\n%s\n' % (gist, msg)
                   )


if __name__ == '__main__':
    t0 = time()

    log('GeoNewsNet v2', 'https://github.com/MarHai/GeoNewsNet')
    log('(c) 2019', 'Mario Haim <mario@haim.it>')

    config = get_config()
    db_engine = get_engine(config)
    db = get_database(db_engine)
    queue = Queue()
    threads = []

    workers = int(config.get('Scraper', 'threads', fallback=4))
    max_depth = int(config.get('Scraper', 'depth', fallback=1))

    for i in range(max_depth + 1):
        log('%d of %d' % (i + 1, max_depth + 1), 'Round of scraping started with %d parallel scrapers' % workers)

        for j in range(workers):
            worker = Scraper(queue, config, db_engine)
            worker.start()
            threads.append(worker)

        outlets = db.query(Outlet).filter(Outlet.scrape_uid.is_(None)).all()
        outlet_string = ''
        for outlet in outlets:
            queue.put(outlet)
            outlet_string = outlet_string + str(outlet) + '\n'
        if len(outlets) > 0:
            log('%d outlets (nodes) added to scraper' % len(outlets), outlet_string, True)

        # for all Outlet-related Scrape objects (level=1), start the recursive process for all Link objects (level=2)
        if max_depth > 1:
            links = db.query(Link).filter(
                Link.scrape_origin_uid == Scrape.uid,
                Scrape.uid == Outlet.scrape_uid,
                Outlet.scrape_uid.isnot(None)
            ).all()
            links_actually_added_to_queue = recursively_add_links_to_queue(queue, 2, links, max_depth)
            if links_actually_added_to_queue > 0:
                log(
                    '%d not-yet-visited links added to scraper' % links_actually_added_to_queue,
                    'Maximum depth is %d' % max_depth,
                    True
                )

        for worker in threads:
            queue.put('quit')
        for worker in threads:
            worker.join()

        db.commit()
        db.close()
        db = get_database(db_engine)

    scrape_total = db.query(func.count(Scrape.uid)).one()[0]
    scrape_successful = db.query(func.count(Scrape.uid)).filter(Scrape.status_code == 200).one()[0]
    statistics = '%d websites scraped, %d of which (%d%%) were successful (i.e., status code 200)' % \
                 (scrape_total, scrape_successful, (0 if scrape_total == 0 else 100*scrape_successful/scrape_total))
    scrape_host_result = db.query(func.count(Link.uid)).group_by(Link.fld_origin)
    if scrape_host_result.count() == 0:
        statistics += '\n' + 'no hosts scraped'
    else:
        scrape_min_documents = min(count[0] for count in scrape_host_result.all())
        scrape_max_documents = max(count[0] for count in scrape_host_result.all())
        statistics += '\n' + (
                '%d hosts scraped, containing between %d and %d documents' %
                (scrape_host_result.count(), scrape_min_documents, scrape_max_documents)
        )
    try:
        links_internal = db.query(func.count(Link.uid)).filter(Link.is_internal).one()[0]
        links_external = db.query(func.count(Link.uid)).filter(Link.is_internal.is_(False)).one()[0]
    except:
        links_internal = 0
        links_external = 0
    links_total = links_internal + links_external
    statistics += '\n' + (
            '%d links collected, %d of which are external (%d%%)' %
            (links_total, links_external, (0 if links_total == 0 else 100 * links_external / links_total))
    )
    links_outlet_direct = db.query(Link.uid, Outlet.uid).filter(
        Link.scrape_target_uid == Outlet.scrape_uid,
        Link.is_internal.is_(False)
    ).count()
    statistics += '\n' + (
            '%d external links (%d%% out of %d external links) link directly to pre-configured outlet pages' % (
                links_outlet_direct,
                (0 if links_external == 0 else (100 * links_outlet_direct / links_external)),
                links_external
            )
    )
    links_outlet_host = db.query(Link.uid, Outlet.uid).filter(
        Link.fld_target == Outlet.fld,
        Link.is_internal.is_(False)
    ).count()
    statistics += '\n' + (
            '%d external links (%d%% out of %d external links) link to outlet hosts' % (
                links_outlet_host,
                (0 if links_external == 0 else (100 * links_outlet_host / links_external)),
                links_external
            )
    )
    scrape_link_result = db.query(func.count(Link.uid)).group_by(Link.scrape_origin_uid)
    if scrape_link_result.count() > 1:
        mean_number_of_links = mean(count[0] for count in scrape_link_result.all())
        sd_number_of_links = stdev(count[0] for count in scrape_link_result.all())
        statistics += '\n' + (
                'on average, scrapes resulted in M = %.1f links (SD = %.1f)' %
                (mean_number_of_links, sd_number_of_links)
        )
        below_10_number_of_links = sum((1 if count[0] < 10 else 0) for count in scrape_link_result.all())
        statistics += '\n' + ('%d scrapes have less than 10 links' % below_10_number_of_links)

    log('Scrape done in %.2f seconds' % (time() - t0), statistics + '\n', True)
