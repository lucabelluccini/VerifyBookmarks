import requests
from requests.adapters import HTTPAdapter

import re
from bs4 import BeautifulSoup

from joblib import Parallel, delayed

bookmark_file = """bookmarks_05_03_16.html"""


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
        status = 'Redirected' if url != r.url else str(r.status_code)
        if status == 'Redirected' and url[4:] == r.url[5:]:
            status = 'HttpsRedirected'
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
        if result[0] == 'HttpsRedirected':
            anchor['href'] = result[2]
        elif result[0] == 'Redirected':
            anchor['href'] = result[2]


if __name__ == "__main__":
    # Prepare a session to set a max retry policy and increase http connections pool size
    session = requests.Session()
    session.mount('http', HTTPAdapter(max_retries=5, pool_maxsize=16))

    # Parse file to obtain html tree
    html_tree = get_html_tree_from_file(bookmark_file)

    # Iterate over the anchors
    Parallel(n_jobs=8, backend='threading', verbose=5)(delayed(process_anchor)(anchor_to_validate, session)
                                                       for anchor_to_validate in extract_anchors_from_tree(html_tree))

    # Final file
    output_html = html_tree.prettify("utf-8")
    with open(bookmark_file + '.clean', 'w') as new_file:
        new_file.write(output_html)
