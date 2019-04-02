# GeoNewsNet v2: Digital News Agendas in Scandinavia
This selection of tools allows to investigate how Scandinavian online news organizations link across geography, ownership, local/regional/national scope, funding models, and the like.

## Manual
1. Clone this repo and install prerequisites:
    ```
    git clone https://github.com/MarHai/GeoNewsNet
    cd GeoNewsNet/
    pip install -r requirements.txt
    ``` 
1. Configure the installation through the `config.ini` file (see below).
1. Set up the database (make sure that the `config.ini` is okay and your Google Doc is ready):
    ```
    python setup.py
    ```
1. Collect data (seriously, this takes a while):
    ```
    python scrape.py
    ```
1. Generate [Gephi](https://gephi.org/) graph file for analysis:
    ```
    python visualize.py
    ```

## Technological background
### Configuration
Change the config file to work with your domain of study. It follows a strict [INI format](https://en.wikipedia.org/wiki/INI_file) with these sections and keys:
- Database
    - *Dialect* tells SQLAlchemy how to talk. Default: `mysql+pymysql`
    - *Host* is the central database host to connect to.
    - *User* must hold the username to connect to the central database.
    - *Password* holds, well, the according password.
    - *Database* represents the database name.
    - Due to long runtimes, these tools sometimes struggle with MySQL server timeouts (at least, if servers close connections rather strictly). To overcome this problem, you may set *Timeout* to a number of seconds after which the database connection should be automatically renewed. Best practice is to do nothing until you run into problems. If you do, however, check your MySQL server's timeout and set the *Timeout* setting to a value slightly below (e.g., -10) this number: 
        ```
        SHOW SESSION VARIABLES LIKE 'wait_timeout';
        ```
- Email
    - *Host* is the address of the SMTP (!) server.
    - *Port* represents the port through which to connect (typically, this is 25 for non-TLS and 465 or 587 for TLS servers).
    - *TLS* should indicate whether a secure TLS connection should be used (1) or not (0).
    - *User* is the user to connect to the SMTP server.
    - *Password*, well, again, holds the according password.
    - *Sender* is the sender's email address to be used for emails.
    - *Recipient* is the recipient's email address to be informed when the scraping process is over.
- Google
    - *Sectors* specifies the complete (!) URL of the downloadable Google Sheet that holds the sectors. This can be acquired through making the sheet publicly available to everyone with the URL (Google teminology: "share"), copying/pasting the URL, and appending a `&output=csv` at the end.
    - *Sectors_have_headers* defines whether the sectors' first row should be skipped.
    - *Outlets* specifies the Google-Sheet URL to the outlets. 
    - *Outlets_have_headers* defines whether the outlets' first row should be skipped.
- Scraper
    - *UserAgent* depicts the user-agent string to use for scraping.
    - *Maintainer* is the name of the person in charge, pushed as "from" via any scraping request's header.
    - *Threads* defines the number of parallel threads to use for scraping (this makes things quicker but requires computational cores).
    - *Parser* is the [BeautifulSoup parser](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to use (default is `lxml`)
    - *Depth* specifies the levels for which the scraper follows links (be careful here as this increases the workload tremendously very quickly; only go beyond 3-4 if you really know what you're doing).

### Database
All database communication is handled through [SQLAlchemy](https://docs.sqlalchemy.org/en/latest/), meaning that you can put a variety of SQL-based database infrastructures below it. Default's to MySQL, however.

The main storage, then, consists of the following tables:
- *Sector* specifies the hierarchical tree structure of sectors to which outlets belong.
- *Outlet* holds the later-to-be-visualized starting points (i.e., nodes) including their geographical positions and an initial URL.
- *Scrape* holds one entry per actual website scraping process. The time it takes for a website to be loaded is logged into this table as well (_seconds_elapsed_). Initial scrapes are also linked to their corresponding outlet elements.
- *Link* finally is the largest table and holds all connections (i.e., edges). It also determines whether a connection is internal or external as well as whether scraping its target resulted in errors (_erroneous_scrapes_).

### Collection procedure
Starting with all outlet entries table, the main _scrape.py_ script follows this general logic:
- For each of the defined outlets, continuously follow all already collected links/edges to check whether the maximum depth of scraping has been reached.
- For every missing page/node, retrieve its website and extract all links from it (working in parallel manner through multi-threading).
    - Scraping is handled through the [requests](http://docs.python-requests.org/en/master/) package.
    - Store every website retrieval inside the *Scrape* database table.
    - Store every extracted link inside the *Link* database table. 
- Repeat this process for a total of `maximum depth of scraping + 1` times.
- At the end of this process, an email is being sent informing about the state of progress.

Final word of warning: Increasing the maximum depth of scraping has a tremendous effect on this script's efficiency. That is, a depth as low as `depth = 2` with only one starting outlet can easily yield 1,000 websites.

### Graph creation
Starting with all a priori specified outlets, the generated `.gexf` file contains all these outlets as nodes along with their number of internal links as well as the ratio between external and internal links (thus warning about nodes without internal links). The file also contains all external links, adequately weighted, between these outlets. Since this builds upon previously collected and stored data, remember to do this after you have collected data.

## Context & History
These tools are part of the [Digital News Agendas in Scandinavia](https://www.uis.no/research-and-phd-studies/research-areas/society-culture-and-religion/digital-news-agendas-in-scandinavia/) project.

A former version of these tools is available as [GeoNewsNet - the Karlstad-Bergen model of how online news sites link each other](https://github.com/eiriks/GeoNewsNet) and has been used for collecting and visualizing data for the following project publication:

> Sjøvaag, H., Stavelin, E., Karlsson, M., & Kammer, A. (2018). The hyperlinked Scandinavian news ecology: The unequal terms forged by the structural properties of digitalisation. _Digital Journalism_, Advance Online Publication. https://doi.org/10.1080/21670811.2018.1454335

## Contact
For this second version of these project tools, [Mario Haim](https://haim.it) is the go-to contact person. 
