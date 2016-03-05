import requests
import re
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

bookmark_file = """bookmarks_05_03_16.html"""


def get_html_tree_from_file(filename):
    with open(filename, 'r') as xml_file:
        return BeautifulSoup(xml_file, 'lxml')


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
        return result_build(url, r.url, status)
    except requests.ConnectionError as cex:
        return result_build(url, url, 'ConnectionError: {}'.format(cex.message))
    except requests.exceptions.ReadTimeout as rtx:
        return result_build(url, url, 'ReadTimeout: {}'.format(rtx.message))
    except requests.exceptions.RequestException as rex:
        return result_build(url, url, 'RequestException: {}'.format(rex.message))
    except Exception as ex:
        return result_build(url, url, ex.message)


def generate_new_bookmark_file(filename, replace_map):
    new_filename = filename + '.clean'
    with open(new_filename, 'w') as new_xml_file:
        with open(filename, 'r') as xml_file:
            for line in xml_file.readlines():
                matches = re.match(r'HREF=\"(http.*?)\" ', line)
                if matches and matches.group(1) in replace_map:
                    line.replace(matches.group(1), replace_map[matches.group(1)]['redirect_url'])
                new_xml_file.write(line)


def process_anchor(anchor, request_session):
    print anchor
    result = verify_url(request_session=request_session, url=anchor['href'])
    print result
    if result[0] == 'HttpsRedirected':
        anchor['href'] = result[2]
    elif result[0] == 'Redirected':
        anchor['href'] = result[2]


if __name__ == "__main__":

    # Prepare a session to set a max retry policy and increase http connections pool size
    session = requests.Session()
    session.mount('http', HTTPAdapter(max_retries=5, pool_maxsize=100))

    # Parse file to obtain html tree
    html_tree = get_html_tree_from_file(bookmark_file)

    # Iterate over the anchors
    for anchor_to_validate in extract_anchors_from_tree(html_tree):
        process_anchor(anchor_to_validate, session)

    # Final file
    print html_tree.prettify("utf-8")

