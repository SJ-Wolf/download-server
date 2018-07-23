import os
import pickle
import sqlite3
import asyncio
import time
from multiprocessing import JoinableQueue, Lock, Process

import pandas as pd
import requests
from aiohttp import web
from fake_useragent import UserAgent
from lxml import html


def ensure_directory(dir):
    if dir == '':
        return
    if not os.path.exists(dir):
        os.makedirs(dir)


def get_latest_free_proxy_list():
    response = requests.get('https://free-proxy-list.net/')
    response.raise_for_status()
    tree = html.fromstring(response.text)
    table_elem = tree.xpath('//table[@id="proxylisttable"]')[0]
    headers = table_elem.xpath('./thead/tr/th/text()')

    row_elems = table_elem.xpath('./tbody/tr')

    data = []
    for row_elem in row_elems:
        row = []
        for col in row_elem.xpath('./td/text()'):
            row.append(col)
        assert len(row) == len(headers)
        data.append(row)

    df = pd.DataFrame(data, columns=headers)
    df = df.where((df['Anonymity'] == 'anonymous') | (df['Anonymity'] == 'elite proxy')).dropna()
    return (df['IP Address'] + ':' + df['Port']).values.tolist()


def get_proxy_server_session(proxy_str):
    proxies = {
        'http': proxy_str
    }
    s = requests.session()
    s.proxies = proxies
    r = s.get("http://api.ipify.org/")
    assert proxy_str.split(':')[0] in r.text
    return s


def get_page_source(url, requests_session=None, wait_time=0, binary=False):
    while True:
        if wait_time > 0:
            time.sleep(wait_time)
        if requests_session is None:
            r = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        else:
            r = requests_session.get(url, headers={'User-Agent': UserAgent().chrome})
        # print(r.headers)
        if r.status_code == 429:
            retry_after = int(r.headers['Retry-After'])
            print(f'Hit limit! Waiting {retry_after} seconds.')
            time.sleep(retry_after)
            continue
        if r.status_code == 404:
            return None
        else:
            r.raise_for_status()
            break
    if binary:
        page_source = r.content
    else:
        page_source = r.text
    return page_source


def get_file_name_from_url(url):
    return os.path.join('html', url.replace('http://', '').replace('https://', '')).replace('\\', '/')


def download_url_queue_into_db(db_lock, url_queue, proxy_str, wait_time, proc_num, to_db, replace_existing):
    try:
        session = get_proxy_server_session(proxy_str)
    except AssertionError:
        return  # proxy dead on arrival :(
    except Exception as e:
        if 'Cannot connect to proxy' in str(e):
            return
        else:
            raise
    print(proxy_str)
    while True:
        # time.sleep(0.1)
        url = url_queue.get()
        print(proc_num, '\t', url)
        # with db_lock:
        try:
            # get_url(url, requests_session=session, verbose=True, overwrite=True)
            if to_db:
                if not replace_existing and url_in_db(url):
                    continue

                page_source = get_page_source(url, requests_session=session, wait_time=wait_time, binary=False)
                if page_source is None:
                    page_source = ''

                with db_lock:
                    with sqlite3.connect('html.db') as db:
                        if replace_existing:
                            db.execute('replace into html values (?, ?)', (url, page_source))
                        else:
                            db.execute('insert or ignore into html values (?, ?)', (url, page_source))
            else:
                file_name = get_file_name_from_url(url)
                if file_name[-1] == '/':
                    file_name = file_name[:-1]
                ensure_directory('/'.join(file_name.split('/')[:-1]))
                if replace_existing or not os.path.exists(file_name):
                    page_source = get_page_source(url, requests_session=session, wait_time=wait_time, binary=True)

                    with open(file_name, 'wb') as f:
                        f.write(page_source)
        except Exception as e:
            print(e)
            if '404' in str(e):
                pass
            else:
                url_queue.put(url)
                break
        url_queue.task_done()


def start_download_workers(proxy_strings, wait_time_between_requests, to_db=True,
                           replace_existing=True, connections_per_server=1):
    if len(proxy_strings) == 0 or connections_per_server == 0:
        return None
    url_queue = JoinableQueue(maxsize=0)
    lock = Lock()
    workers = []

    for proc_num, proxy_str in enumerate(proxy_strings):
        for i in range(connections_per_server):
            worker = Process(target=download_url_queue_into_db,
                             args=(lock, url_queue, proxy_str, wait_time_between_requests, proc_num, to_db, replace_existing))
            worker.daemon = True
            worker.start()
            workers.append(worker)
    # for url in urls:
    #     time.sleep(wait_time_between_adding_urls)
    #     url_queue.put(url)

    return url_queue, workers


def stop_download_workers(url_queue, workers):
    # for i in range(num_threads):
    #     q.put('DONE')
    print('joining queue')
    url_queue.join()

    print('joining processes')
    [w.terminate() for w in workers]


def get_url_if_exists(url, return_when_does_not_exist=None):
    html_cursor.execute(f'select html from html where url = "{url}" limit 1')
    r = html_cursor.fetchall()
    if len(r) == 0:
        return return_when_does_not_exist
    else:
        return r[0][0]


def url_in_db(url):
    html_cursor.execute(f'select url from html where url = "{url}" limit 1')
    r = html_cursor.fetchall()
    if len(r) == 0:
        return False
    else:
        return True


async def download_handle(request):
    # return web.Response(body='asdf')
    # print(f"{request.rel_url.query.get('url')}")
    # await asyncio.sleep(2)
    if "url" not in request.rel_url.query:
        return web.Response(status=422, reason='url(s) must be provided!')
    url = request.rel_url.query.get("url")

    if 'wait_for_response' in request.rel_url.query and request.rel_url.query.get('wait_for_response').lower() == 'true':
        response_contents = get_url_if_exists(url, -1)
        if response_contents != -1:
            return web.Response(body=response_contents)
        url_queue.put(url)
        while True:
            time.sleep(0.1)
            response_contents = get_url_if_exists(url, -1)
            if response_contents != -1:
                return web.Response(body=response_contents)
    else:
        url_queue.put(url)
        return web.Response()


# async def handle(request):
#     if "url" not in request.rel_url.query:
#         return web.Response(status=422, reason='url(s) must be provided!')
#     url = request.rel_url.query.get("url")
#     url_queue.put(url)
#     text = f'Hello, you have searched for {url}!\nThere are {len(workers)} workers.'
#     return web.Response(body=text)


async def welcome_handle(request):
    return web.Response(text='Hello!')


url_queue = None
workers = None

html_db = sqlite3.connect('html.db')
html_cursor = html_db.cursor()
html_cursor.execute('create table if not exists html(url text primary key, html text)')
html_cursor.executescript("""
    PRAGMA page_size = 4096;
    -- PRAGMA cache_size = 2652524;
    PRAGMA cache_size = 38400;
    PRAGMA temp_store = MEMORY""")


def run():
    global url_queue, workers

    if abs(int(time.time()) - os.path.getmtime('proxies.pkl') / 60 / 60) > 1:
        with open('proxies.pkl', 'wb') as f:
            proxies = get_latest_free_proxy_list()
            pickle.dump(proxies, f)
    else:
        with open('proxies.pkl', 'rb') as f:
            proxies = pickle.load(f)

    url_queue, workers = start_download_workers(proxies[:100], wait_time_between_requests=5, to_db=True,
                                                replace_existing=False, connections_per_server=1)

    app = web.Application()
    app.add_routes([web.get('/', welcome_handle),
                    web.get('/get_url', download_handle),
                    ])

    web.run_app(app)

    stop_download_workers(url_queue, workers)

    html_cursor.close()
    html_db.close()


if __name__ == '__main__':
    run()
