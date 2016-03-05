import xml.etree.ElementTree as ET
from joblib import Parallel, delayed
import requests
import mmap
import re
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

#test

bookmark_file = """C:\\Users\\Luca\\Desktop\\bookmarks_03_03_16.html"""

def extract_bookmark_urls_html(filename):
    with open(filename, 'r') as xml_file:
        soup = BeautifulSoup(xml_file, 'lxml')
        for a_element in soup.findAll('a', href=re.compile('http')):
            yield a_element

def extract_bookmark_urls_regexp(filename):
    with open(filename, 'r+') as xml_file:
        data = mmap.mmap(xml_file.fileno(), 0)
        for result in re.finditer(r'HREF=\"(http.*?)\" ', data, re.MULTILINE):
            yield result.group(1)

def verify_url(url, session):
    def result_build(original_url, final_url, reason):
        return { original_url : { 'redirect_url': final_url, 'reason_or_status': reason } }
    try:
        r = session.get(url, allow_redirects=True, timeout=5, stream=True)
        redirect_info = 'Redirected' if url != r.url else None
        if redirect_info == 'Redirected' or r.status_code != 200:
            if url[4:] == r.url[5:]:
                redirect_info = 'HttpsRedirected'
            result = result_build(url, r.url, redirect_info if redirect_info else r.status_code)
        else:
            result = None
    except requests.ConnectionError as ce:
        result = result_build(url, url, 'ConnectionError: {}'.format(ce.message))
    except requests.exceptions.ReadTimeout as rt:
        result = result_build(url, url, 'ReadTimeout: {}'.format(rt.message))
    except requests.exceptions.RequestException as re:
        result = result_build(url, url, 'RequestException: {}'.format(re.message))
    except Exception as e:
        result = result_build(url, url, e.message)
    return result


def generate_new_bookmark_file(filename, replace_map):
    new_filename = filename + '.clean'
    with open(new_filename, 'w') as new_xml_file:
        with open(filename, 'r') as xml_file:
            for line in xml_file.readlines():
                matches = re.match(r'HREF=\"(http.*?)\" ', line)
                if matches and matches.group(1) in replace_map:
                    line.replace(matches.group(1), replace_map[matches.group(1)]['redirect_url'])
                new_xml_file.write(line)


if __name__ == "__main__":

    session = requests.Session()
    session.mount('http', HTTPAdapter(max_retries=5, pool_maxsize=100))

    for x in extract_bookmark_urls_html(bookmark_file):
        print x
    exit()
    results = Parallel(n_jobs=64, verbose=5)(delayed(verify_url)(url, session) for url in extract_bookmark_urls_regexp(bookmark_file))

    results = filter(None, results)

    replacement_map = { k: v for d in results for k, v in d.iteritems() if v['reason_or_status'] == 'HttpsRedirected' }
