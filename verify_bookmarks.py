import requests
from requests.adapters import HTTPAdapter
import re
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import logging

bookmark_file = """bookmarks_05_03_16.html"""
logging.basicConfig(filename='trace.log', level=logging.DEBUG)
logger = logging.getLogger('bookmarks')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.FileHandler(filename='bookmarks.log'))


def get_html_tree_from_file(filename):
    with open(filename, 'r') as xml_file:
        return BeautifulSoup(xml_file, "html.parser")


def extract_anchors_from_tree(tree):
    for a_element in tree.findAll('a', href=re.compile('http')):
        yield a_element


def verify_url(url, request_session):
    def result_build(original_url, final_url, reason):
        return reason, original_url, final_url

    try:
        r = request_session.get(url, allow_redirects=True, timeout=5, stream=True)
        if r.history:
            for req in r.history:
                status = req.reason
        else:
            status = r.reason
        r.close()
        return result_build(url, r.url, status)
    except requests.ConnectionError as cex:
        return result_build(url, url, 'ConnectionError: {}'.format(cex.message))
    except requests.exceptions.ReadTimeout as rtx:
        return result_build(url, url, 'ReadTimeout: {}'.format(rtx.message))
    except requests.exceptions.RequestException as rex:
        return result_build(url, url, 'RequestException: {}'.format(rex.message))
    except Exception as ex:
        return result_build(url, url, ex.message)


def process_anchor(anchor, request_session):
    result = verify_url(request_session=request_session, url=anchor['href'])
    if result:
        logger.debug(str(result))
        if result[0] == 'PermanentRedirect':
            anchor['href'] = result[2]
        elif result[0] == 'Redirect':
            anchor['href'] = result[2]


if __name__ == "__main__":
    # Prepare a session to set a max retry policy and increase http connections pool size
    session = requests.Session()
    session.mount('http', HTTPAdapter(max_retries=5, pool_maxsize=16))

    # Parse file to obtain html tree
    html_tree = get_html_tree_from_file(bookmark_file)

    # Iterate over the anchors
    Parallel(n_jobs=64, backend='threading', verbose=5)(delayed(process_anchor)(anchor_to_validate, session)
                                                       for anchor_to_validate in extract_anchors_from_tree(html_tree))

    # Final file... I know it is not safe but the NETSCAPE-Bookmark-file-1 needs html markup in uppercase...
    adapted_html = str(html_tree)
    with open(bookmark_file + '.clean.html', 'w') as new_file:
        adapted_html = adapted_html.replace('</dt>', '').replace('</p>', '').replace('</meta>', '')
        adapted_html = adapted_html.replace('h3>', 'H3>')\
            .replace('</a>', '</A>').replace('<a', '<A').replace('dt>', 'DT>').replace('dl>', 'DL>')\
            .replace('<h1', '<H1').replace('h1>', 'H1>').replace('<h3', '<H3').replace('<meta', '<META')\
            .replace('title>', 'TITLE>').replace('add_date=', 'ADD_DATE=').replace('icon=', 'ICON=')\
            .replace('content=', 'CONTENT=').replace('http-equiv=', 'HTTP-EQUIV=')\
            .replace('last_modified=', 'LAST_MODIFIED').replace('personal_toolbar_folder=', 'PERSONAL_TOOLBAR_FOLDER=')\
            .replace('href=', 'HREF=')

        new_file.write(adapted_html)
