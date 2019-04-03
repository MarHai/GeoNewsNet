from time import time
import requests
import configparser
import sys
import traceback
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
import csv
from database import base, Outlet, Scrape, Link, Sector
from tld.utils import update_tld_names
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def get_config(ini_file='config.ini'):
    config = configparser.RawConfigParser()
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
                '/' + config.get('Database', 'database', fallback='geonewsnet') + \
                '?charset=utf8'
    print('- connecting to %s' % connector)
    try:
        database_timeout = int(config.get('Database', 'timeout', fallback=-1))
        if database_timeout > 0:
            print('- reconnecting (through SQLAlchemy\'s pool_recycle) every %d seconds' % database_timeout)
    except:
        database_timeout = -1
    try:
        return create_engine(connector, encoding='utf8', pool_recycle=database_timeout, max_overflow=-1)
    except:
        die_with_error('Database engine could not be created')


def get_database(engine):
    try:
        session_factory = sessionmaker(bind=engine)
        db = scoped_session(session_factory)
        db.execute('SET NAMES "UTF8"')
        db.execute('SET CHARACTER SET "UTF8"')
        return db
    except:
        die_with_error('Database session could not be initiated')


def import_sectors(config, db):
    sheet = config.get('Google', 'sectors')
    if sheet is not '':
        print('- retrieving sectors from Google Drive %s' % sheet)
        try:
            response = requests.get(sheet)
            if response.status_code == 200:
                csv_raw = response.content.decode('utf8')
                csv_list = csv_raw.splitlines()
                csv_data = csv.DictReader(csv_list,
                                          fieldnames=['parent', 'name'],
                                          dialect=csv.Sniffer().sniff(csv_list[0]))
                if config.get('Google', 'sectors_have_headers') == '1':
                    next(csv_data, None)
                    print('- skipping header row')
                counter_new = 0
                counter_update = 0
                for entry in csv_data:
                    sector_name = entry['name'].strip()
                    temp_sector = db.query(Sector).filter(Sector.name == sector_name).one_or_none()
                    temp_parent = None
                    if entry['parent'] != '':
                        temp_parent = db.query(Sector).filter(Sector.name == entry['parent'].strip()).one_or_none()
                        if temp_parent is None:
                            print('- parent "%s" not found, sector attached to root level' % entry['parent'])
                    if temp_sector is None:
                        temp_sector = Sector(name=sector_name)
                        if temp_parent is not None:
                            temp_sector.parent_uid = temp_parent.uid
                        db.add(temp_sector)
                        counter_new = counter_new + 1
                    else:
                        if temp_parent is None and temp_sector.parent_uid is not None:
                            temp_sector.parent_uid = None
                            counter_update = counter_update + 1
                        elif temp_parent is not None and temp_parent.uid is not temp_sector.parent_uid:
                            temp_sector.parent_uid = temp_parent.uid
                            counter_update = counter_update + 1
                    db.commit()
                print('- imported %d new sectors' % counter_new)
                print('- updated %d sectors' % counter_update)
            else:
                die_with_error('Google Drive returned unexpected status code %d' % response.status_code)
        except:
            die_with_error('Google Drive could not be contacted properly')


def import_outlets(config, db):
    sheet = config.get('Google', 'outlets')
    if sheet is not '':
        print('- retrieving outlets from Google Drive %s' % sheet)
        try:
            response = requests.get(sheet)
            if response.status_code == 200:
                csv_raw = response.content.decode('utf8')
                csv_list = csv_raw.splitlines()
                csv_data = csv.DictReader(csv_list,
                                          fieldnames=['url', 'name', 'area',
                                                      'level', 'sector', 'subsector', 'owner',
                                                      'reach', 'reach_unit', 'founding_year', 'revenue', 'topic',
                                                      'notes',
                                                      'latitude', 'longitude'],
                                          dialect=csv.Sniffer().sniff(csv_list[0]))
                if config.get('Google', 'outlets_have_headers') == '1':
                    next(csv_data, None)
                    print('- skipping header row')
                counter_new = 0
                counter_update = 0
                for entry in csv_data:
                    outlet_url = Link.sanitize_url(entry['url'].strip(), base_url='')
                    if outlet_url == '':
                        print('- urgs, URL "%s" could not be parsed correctly and was skipped' % entry['url'])
                        continue
                    temp_outlet = db.query(Outlet).filter(Outlet.url == outlet_url).one_or_none()
                    temp_sector = None
                    if entry['subsector'] != '':
                        temp_sector = db.query(Sector).filter(Sector.name == entry['subsector']).one_or_none()
                    if temp_outlet is None:
                        temp_outlet = Outlet(url=outlet_url)
                        db.add(temp_outlet)
                        counter_new = counter_new + 1
                    else:
                        counter_update = counter_update + 1
                    temp_outlet.name = entry['name'].strip()
                    temp_outlet.area = Outlet.sanitize_area(entry['area'])
                    if temp_sector is not None:
                        temp_outlet.sector_uid = temp_sector.uid
                    temp_outlet.ownership = entry['owner'].strip()
                    temp_outlet.level = Outlet.sanitize_level(entry['level'])
                    temp_outlet.reach = int(float(entry['reach'])) if entry['reach'] != '' else None
                    temp_outlet.reach_unit = entry['reach_unit'].strip() if entry['reach_unit'] != '' else None
                    temp_outlet.founding_year = int(entry['founding_year']) if entry['founding_year'] != '' else None
                    temp_outlet.revenue = entry['revenue'].strip() if entry['revenue'] != '' else None
                    temp_outlet.topic = entry['topic'].strip() if entry['topic'] != '' else None
                    temp_outlet.note = entry['notes'].strip() if entry['notes'] != '' else None
                    temp_outlet.latitude = float(entry['latitude']) if entry['latitude'] != '' else None
                    temp_outlet.longitude = float(entry['longitude']) if entry['longitude'] != '' else None
                    temp_outlet.fld = Link.extract_fld(outlet_url)
                db.commit()
                print('- imported %d new outlets' % counter_new)
                print('- updated %d outlets' % counter_update)
            else:
                die_with_error('Google Drive returned unexpected status code %d' % response.status_code)
        except:
            die_with_error('Google Drive could not be contacted properly')


def get_browser_header(config):
    return {
        'user-agent': config.get('Scraper', 'useragent'),
        'from': config.get('Scraper', 'maintainer')
    }


def get_mailer(config):
    use_tls = True if config.get('Email', 'tls', fallback=0) else False
    host = config.get('Email', 'host')
    port = int(config.get('Email', 'port', fallback=(465 if use_tls else 25)))
    context = ssl.create_default_context()
    server = None
    try:
        if use_tls:
            print('- connecting to %s using TLS via port %d' % (host, port))
            server = smtplib.SMTP(host, port)
            server.starttls(context=context)
        else:
            print('- connecting to %s via port %d' % (host, port))
            server = smtplib.SMTP_SSL(host, port, context=context)
        server.login(config.get('Email', 'user'), config.get('Email', 'password'))
    except:
        die_with_error('connection to SMTP server failed')
    return server


def send_email(config, subject, message, attachments=None):
    sender = config.get('Email', 'sender')
    recipient = config.get('Email', 'recipient')
    print('- attempting to send an email ("%s") to %s' % (subject, recipient))
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(message, 'plain'))
    if attachments is not None:
        for attachment in attachments:
            try:
                with open(attachment, 'rb') as file:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(file.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment; filename=%s' % attachment)
                    msg.attach(part)
                    print('- attaching "%s" to the email' % attachment)
            except:
                print('- "%s" could not be attached to email' % attachment)
    try:
        mailer = get_mailer(config)
        mailer.sendmail(sender, recipient, msg.as_string())
    except:
        die_with_error('email could not be sent')


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

    print('Checking SMTP/email setup')
    send_email(config, '[GeoNewsNet] Test Message from Setup Process',
               'Hi,\n\nthis is just a test message to let you know that the email sending actually works!\n' +
               'Please note the completely uninteresting file attached.\n\n' +
               'Thanks for using our tools,\nMario\n',
               ['requirements.txt'])
    print('---------')

    print('Checking scraper')
    check_request()
    print('---------')

    print('Updating top-level-domain list')
    update_tld_names()
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
    import_sectors(config, db)
    import_outlets(config, db)
    print('---------')

    print('Done in %.2f seconds' % (time() - t0))
