from sqlalchemy import Column, String, Integer, Text, Boolean, Numeric, func, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from tld import get_tld, get_fld
from tld.exceptions import TldBadUrl, TldDomainNotFound
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests

base = declarative_base()


class ScrapeError(Exception):
    response = None


class Scrape(base):
    __tablename__ = 'scrape'
    uid = Column(Integer, primary_key=True)
    created = Column(DateTime, default=func.now())
    url_started = Column(Text, nullable=False)
    url_finished = Column(Text)
    status_code = Column(Integer)
    seconds_elapsed = Column(Numeric(10, 8), nullable=False)
    outlet = relationship('Outlet', back_populates='scrape')
    links_outgoing = relationship(
        'Link',
        back_populates='scrape_origin',
        order_by='Link.uid',
        foreign_keys='[Link.scrape_origin_uid]'
    )
    links_incoming = relationship(
        'Link',
        back_populates='scrape_target',
        order_by='Link.uid',
        foreign_keys='[Link.scrape_target_uid]'
    )

    def __repr__(self):
        return "<Scrape('%s', scraped='%s', status='%d')>" % (self.url_finished, self.created, self.status_code)

    @staticmethod
    def filter_link_tags(a):
        if a and a.name.lower() == 'a' and hasattr(a, 'get'):
            href = a.get('href')
            if href and href is not None:
                return all([
                    not a.has_attr('no_track'),
                    not href.startswith((
                        'mailto:', 'ftp:', 'tlf:', 'tel:', 'sip:', 'sms:', 'webcal:', 'file:',
                        '#', 'javascript:'
                    )),
                    not href.endswith((
                        '.jpg', '.jpeg', '.png', '.gif', '.bmp',
                        '.mov', '.mp4', '.avi',
                        '.pdf', '.doc', '.xls', '.docx', '.xlsx'
                    ))
                ])
        return False

    @staticmethod
    def extract(html, url, parser='lxml'):
        soup = BeautifulSoup(html, parser)
        links = []
        for link in soup.find_all(Scrape.filter_link_tags):
            link = Link.sanitize_url(link.get('href'), base_url=url)
            if link and link not in links:
                links.append(link)
        return links

    @staticmethod
    def request(url, browser_header=None):
        # gangster mode on
        # verify=False bypasses HTTPS certificate verification
        import warnings
        from urllib3.connectionpool import InsecureRequestWarning
        # this is generally not advisable at all and legitimately raises lots of insecurity warnings
        # having said that, we do not want to see all these (completely legit but redundant) warnings
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=InsecureRequestWarning)
            response = requests.get(url, headers=browser_header, verify=False)
        # gangster mode off
        if response.status_code == 200:
            return response
        else:
            error = ScrapeError('Response code (%d) did not yield promising results' % response.status_code)
            error.response = response
            raise error


class Link(base):
    __tablename__ = 'link'
    uid = Column(Integer, primary_key=True)
    url_origin = Column(Text, nullable=False)
    fld_origin = Column(String(250), nullable=False)
    scrape_origin_uid = Column(Integer, ForeignKey('scrape.uid'), nullable=False)
    scrape_origin = relationship(Scrape, back_populates='links_outgoing', foreign_keys=[scrape_origin_uid])
    url_target = Column(Text, nullable=False)
    fld_target = Column(String(250), nullable=False)
    is_internal = Column(Boolean)
    scrape_target_uid = Column(Integer, ForeignKey('scrape.uid'))
    scrape_target = relationship(Scrape, back_populates='links_incoming', foreign_keys=[scrape_target_uid])
    erroneous_scrapes = Column(Integer, default=0, nullable=False)

    def __repr__(self):
        return "<Link(internal='%d', origin='%s', target='%s')>" % (self.is_internal, self.url_origin, self.url_target)

    def increase_errors(self):
        self.erroneous_scrapes = self.erroneous_scrapes + 1

    @staticmethod
    def sanitize_url(url, base_url=''):
        try:
            if base_url is not '' and not url.startswith(('http:', 'https:')):
                url = urljoin(base_url, url)
            url_object = get_tld(url, as_object=True, fix_protocol=True).parsed_url
            return url_object.geturl()
        except TldDomainNotFound:
            return ''
        except TldBadUrl:
            return ''

    @staticmethod
    def extract_fld(url):
        return get_fld(url)

    @staticmethod
    def extract_tld(url):
        return get_tld(url)


class Outlet(base):
    __tablename__ = 'outlet'
    uid = Column(Integer, primary_key=True)
    name = Column(String(150), nullable=False)
    owner = Column(String(150))
    publisher = Column(String(150))
    city = Column(String(80))
    country = Column(String(50), nullable=False)
    area = Column(String(50))
    reach = Column(String(30))
    latitude = Column(Numeric(10, 8), nullable=False)
    longitude = Column(Numeric(11, 8), nullable=False)
    is_composite = Column(Boolean, default=False, nullable=False)
    url = Column(Text, nullable=False)
    fld = Column(String(250), index=True, nullable=False)
    tld = Column(String(7))
    scrape_uid = Column(Integer, ForeignKey('scrape.uid'))
    scrape = relationship(Scrape, back_populates='outlet')

    def __repr__(self):
        return "<Outlet('%s', name='%s', country='%s')>" % (self.fld, self.name, self.country)

    @staticmethod
    def sanitize_country(country):
        country = country[0].upper() + country[1:]
        if country not in ['Denmark', 'Norway', 'Sweden']:
            country = 'Other: ' + country
        return country
