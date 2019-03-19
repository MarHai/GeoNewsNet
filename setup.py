from time import time
import requests
import configparser
import sys
import traceback
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
import csv
from database import base, Outlet, Scrape, Link


def get_config(ini_file='config.ini'):
    config = configparser.ConfigParser()
    print('- reading %s' % ini_file)
    try:
        config.read(ini_file)
        print('- read %d sections successfully: %s' % (len(config.sections()), config.sections()))
        return config
    except:
        die_with_error('%s could not be read' % ini_file)


def get_engine(config):
    connector = config.get('Database', 'dialect', fallback='mysql+pymysql') + \
                '://' + config.get('Database', 'user', fallback='root') + \
                ':' + config.get('Database', 'password', fallback='password') + \
                '@' + config.get('Database', 'host', fallback='localhost') + \
                '/' + config.get('Database', 'database', fallback='geonewsnet')
    print('- connecting to %s' % connector)
    database_timeout = -1
    try:
        database_timeout = int(config.get('Database', 'timeout', fallback=-1))
        if database_timeout > 0:
            print('- reconnecting (through SQLAlchemy\'s pool_recycle) every %d seconds' % database_timeout)
    except:
        database_timeout = -1
    try:
        return create_engine(connector, encoding='utf-8', pool_recycle=database_timeout)
    except:
        die_with_error('Database engine could not be created')


def get_database(engine):
    try:
        session_factory = sessionmaker(bind=engine)
        return scoped_session(session_factory)
    except:
        die_with_error('Database session could not be initiated')


def import_outlets(config, db):
    sheet = config.get('Google', 'outlets')
    if sheet is not '':
        print('- retrieving outlets from Google Drive %s' % sheet)
        try:
            response = requests.get(sheet)
            if response.status_code == 200:
                csv_raw = response.content.decode('utf-8')
                csv_list = csv_raw.splitlines()
                csv_data = csv.DictReader(csv_list,
                                          fieldnames=['url', 'is_composite', 'latitude', 'longitude', 'area', 'name',
                                                      'reach', 'city', 'country', 'owner', 'publisher'],
                                          dialect=csv.Sniffer().sniff(csv_list[0]))
                counter = 0
                for entry in csv_data:
                    temp_outlet = Outlet(
                        name=entry['name'],
                        owner=entry['owner'],
                        publisher=entry['publisher'],
                        city=entry['city'],
                        country=Outlet.sanitize_country(entry['country']),
                        area=entry['area'],
                        reach=entry['reach'],
                        latitude=float(entry['latitude']),
                        longitude=float(entry['longitude']),
                        is_composite=(entry['is_composite'] is 'yes'),
                        url=Link.sanitize_url(entry['url'], base_url=''),
                        fld=Link.extract_fld(entry['url']),
                        tld=Link.extract_tld(entry['url'])
                    )
                    db.add(temp_outlet)
                    counter = counter + 1
                db.commit()
                print('- imported %d outlets' % counter)
            else:
                die_with_error('Google Drive returned unexpected status code %d' % response.status_code)
        except:
            die_with_error('Google Drive could not be contacted properly')


def get_browser_header(config):
    return {
        'user-agent': config.get('Scraper', 'useragent'),
        'from': config.get('Scraper', 'maintainer')
    }


def check_request(url='https://haim.it'):
    print('- requesting %s' % url)
    header = get_browser_header(config)
    print('- using custom headers %s' % str(header))
    try:
        response = Scrape.request(url)
        if response.history:
            print('- request was redirected to %s' % response.url)
        try:
            links = Scrape.extract(response.text, response.url, config.get('Scraper', 'parser', fallback='lxml'))
            if len(links) > 0:
                print('- found %d links: %s' % (len(links), links))
                print('- this is considered successful')
                return True
            else:
                die_with_error('No links found (at all) on %s' % url)
        except:
            die_with_error('Returned data does not depict valid HTML')
    except:
        die_with_error('Test request to %s did not work' % url)


def die_with_error(msg):
    print('- ERROR: %s' % msg)
    error = sys.exc_info()[0]
    if error is not None:
        print('- ' + str(error))
        print('- ' + traceback.format_exc())
    exit(1)


if __name__ == '__main__':
    t0 = time()

    print('GeoNewsNet v2')
    print('https://github.com/MarHai/GeoNewsNet')
    print('(c) 2019 by Mario Haim <mario@haim.it>')
    print('---------')

    print('Checking config file')
    config = get_config()
    print('---------')

    print('Checking database')
    engine = get_engine(config)
    db = get_database(engine)
    inspector = inspect(engine)
    if len(base.metadata.tables.keys()) != len(inspector.get_table_names()):
        print('- creating database tables')
        base.metadata.create_all(engine)
    else:
        print('- database already contains tables, so nothing is created')
    outlet_count = 0
    try:
        outlet_count = db.query(Outlet).count()
    except:
        print('- outlet table not found')
    if outlet_count == 0:
        print('- no outlets found, importing outlets now ...')
        # @todo: import_outlets(config, db)
        db.add(Outlet(
            name='Haram, Sandøy, Skodje',
            owner='Amedia',
            publisher='ScandMedia',
            city='Møre og Romsdal',
            country=Outlet.sanitize_country('Norway'),
            area='Nordre',
            reach='local',
            latitude=62.595778,
            longitude=6.444845,
            is_composite=False,
            url=Link.sanitize_url('http://nordrenett.no/'),
            fld=Link.extract_fld('http://nordrenett.no/'),
            tld=Link.extract_tld('http://nordrenett.no/')
        ))
        db.commit()
    else:
        print('- %d outlets found, nothing imported' % outlet_count)
    print('---------')

    print('Checking scraper')
    check_request()
    print('---------')

    print('Done in %.2f seconds' % (time() - t0))
