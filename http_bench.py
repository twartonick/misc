"""Asynchronous HTTP server benchmark application.

Based on http://curl.haxx.se/mail/curlpython-2010-08/att-0002/retriever-multi.py
recipe.
"""

__author__ = 'vovanec@gmail.com'


import argparse
import concurrent.futures
import http.client
import logging
import pprint
import pycurl
import statistics
import time

from io import BytesIO


USER_AGENT = 'Mozilla/5.0'

# Number of parallel client connections
DEF_NUM_CONNECTIONS = 10

# Curl connection timeout, seconds
DEF_CONNECTION_TIMEOUT = 15

INFINITY = float('inf')


class HTTPQueryEngine(object):
    """Asynchronous HTTP query engine.
    """

    def __init__(self, url, *, num_connections=DEF_NUM_CONNECTIONS,
                 keep_alive_conn=True, timeout=DEF_CONNECTION_TIMEOUT):

        """Constructor.

        :param str url: HTTP server URL.
        :param int num_connections: the number of simultaneous.
        :param bool keep_alive_conn: whether to pass Connection: keep-alive
               header in HTTP request.
        :param int timeout: connect timeout.
        """

        self.log = logging.getLogger(self.__class__.__name__)

        self.keep_alive_conn = keep_alive_conn
        self.url = url

        self.curl_multi = pycurl.CurlMulti()

        self.curl_handles = []
        for _ in range(num_connections):
            self.curl_handles.append(self._curl_handle_create(timeout))

        self.free_pool = self.curl_handles[:]

    def __call__(self, *, num_requests=INFINITY):
        """Run query engine.

        :rtype: dict
        :return: results dict.
        """

        if self.curl_handles is None:
            raise Exception('Operation on closed object.')

        result = {'req_total': 0,
                  'http_ok': 0,
                  'http_error': 0,
                  'curl_error': 0,
                  '__response_times': []}

        self.log.info('Querying %s with %s simultaneous connections...',
                      self.url, len(self.curl_handles))

        num_in_progress = 0
        num_finished = 0
        
        time_start = time.time()
        try:
            while num_finished < num_requests:
                while self.free_pool and (num_in_progress < num_requests):
                    handle = self.free_pool.pop()
                    self._curl_handle_submit(handle)
                    num_in_progress += 1

                ret = pycurl.E_CALL_MULTI_PERFORM
                while ret == pycurl.E_CALL_MULTI_PERFORM:
                    ret, _ = self.curl_multi.perform()

                num_finished += self._collect_stats(result)

                self.curl_multi.select(0.1)
        except KeyboardInterrupt:
            self.log.info('Done.')

        return self._make_summary_dict(result, time_start)

    def close(self):
        """Close all open handles.
        """

        if self.curl_handles is None:
            raise Exception('Operation on closed object.')

        for curl_handle in self.curl_handles:
            curl_handle.close()

        self.curl_multi.close()

        self.curl_handles = None
        self.free_pool = None
        self.curl_multi = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def _curl_handle_create(timeout):
        """Create new curl handle.

        :param int timeout: connection timeout.
        """

        curl_handle = pycurl.Curl()
        curl_handle.body = None

        curl_handle.setopt(pycurl.NOSIGNAL, 1)
        curl_handle.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V4)
        curl_handle.setopt(pycurl.CONNECTTIMEOUT, timeout)
        curl_handle.setopt(pycurl.TIMEOUT, timeout)
        curl_handle.setopt(pycurl.USERAGENT, USER_AGENT)

        return curl_handle

    def _curl_handle_submit(self, curl_handle, *, request_headers=None):
        """Submit curl handle for polling.

        :param pycurl.Curl curl_handle: curl handle.
        :param dict request_headers: HTTP request headers.
        """

        curl_handle.body = BytesIO()
        curl_handle.headers = BytesIO()
        self._add_req_headers(curl_handle, keep_alive_conn=self.keep_alive_conn,
                              headers=request_headers)
        curl_handle.setopt(pycurl.URL, self.url)
        curl_handle.setopt(pycurl.WRITEFUNCTION, curl_handle.body.write)
        curl_handle.setopt(pycurl.HEADERFUNCTION, curl_handle.headers.write)

        curl_handle.start_ts = time.time()

        self.curl_multi.add_handle(curl_handle)

    def _curl_handle_free(self, curl_handle):
        """Free curl handle.

        :param pycurl.Curl curl_handle: curl handle.
        :return:
        """

        curl_handle.body = None
        curl_handle.headers = None
        curl_handle.start_ts = None

        self.curl_multi.remove_handle(curl_handle)
        self.free_pool.append(curl_handle)

    def _collect_stats(self, result):
        """Check for curl objects which have terminated, collect
        result and add them to the freelist.

        :param dict result: dictionary to collect query results.

        :return: the number of finished queries.
        """

        queries_finished = 0

        while True:
            num_q, ok_list, err_list = self.curl_multi.info_read()
            for handle in ok_list:
                status_code = handle.getinfo(pycurl.HTTP_CODE)

                headers = handle.headers.getvalue()
                body = handle.body.getvalue()
                resp_time = int(1000 * (time.time() - handle.start_ts))
                headers = list(filter(None, headers.split(b'\r\n')))

                self.log.debug('Got response in %s milliseconds. ', resp_time)
                self.log.debug('Response headers:\n%s', pprint.pformat(headers))
                self.log.debug('Response body: %s', body)
                if status_code == http.client.OK:
                    result['http_ok'] += 1
                else:
                    result['http_error'] += 1

                result['__response_times'].append(resp_time)
                self._curl_handle_free(handle)

            for handle, errno, errmsg in err_list:
                self.log.warning('Query for %s failed: %s, %s',
                                 self.url, errno, errmsg)
                self._curl_handle_free(handle)

            queries_finished += (len(ok_list) + len(err_list))
            result['req_total'] += queries_finished
            result['curl_error'] += len(err_list)

            if num_q == 0:
                break

        return queries_finished

    @staticmethod
    def _add_req_headers(curl_handle, *, keep_alive_conn=True, headers=None):
        """Add custom HTTP request headers to the Curl handle.

        :param pycurl.Curl curl_handle: curl handle.
        :param bool keep_alive_conn: whether to pass 'Connection: keep-alive'
               header.
        :param dict headers: HTTP header dictionary.
        """

        header_list = []
        if keep_alive_conn:
            header_list.append('Connection: keep-alive')
        else:
            header_list.append('Connection: close')

        if headers is not None:
            for name, val in headers.items():
                header_list.append('%s: %s' % (name, val))

        curl_handle.setopt(pycurl.HTTPHEADER, header_list)

    @staticmethod
    def _make_summary_dict(result, time_start):
        """Make summary dictionary in format:

            {'req_total': total queries done,
             'http_ok': number of queries with 200 HTTP code,
             'http_error': number of queries with non-200 HTTP code,
             'curl_error': number of requests resulted in curl error,
             'req_min': minimum request time,
             'req_max': maximum request time,
             'req_mean': mean request time,
             'req_median': request time median,
             'req_stdev': request time standard deviation,
             'qps': number of queries per second,
             'time_elapsed': total test time}

        :rtype: dict
        """

        time_elapsed = time.time() - time_start
        result['time_elapsed'] = time_elapsed

        if len(result['__response_times']) > 1:
            result['qps'] = (result['http_error'] +
                             result['http_ok']) / time_elapsed
            result['req_min'] = min(result['__response_times'])
            result['req_max'] = max(result['__response_times'])
            result['req_mean'] = statistics.mean(result['__response_times'])
            result['req_median'] = statistics.median(result['__response_times'])
            result['req_stdev'] = statistics.stdev(result['__response_times'])

        return result


def _aggregate_worker_stats(result_dicts):
    """Aggregate summary dictionaries from worker processes.

    :param list result_dicts: the list of summary dictionaries.
    :rtype: dict
    """

    time_total = 0
    queries_total = 0
    http_ok = 0
    http_error = 0
    curl_error = 0
    response_times = []

    for res_dict in result_dicts:
        time_total += res_dict['time_elapsed']
        http_ok += res_dict['http_ok']
        http_error += res_dict['http_error']
        curl_error += res_dict['curl_error']
        queries_total += res_dict['http_error']
        queries_total += res_dict['http_ok']
        response_times += res_dict['__response_times']

    time_elapsed = time_total / len(result_dicts)

    result = {'http_ok': http_ok,
              'http_error': http_error,
              'curl_error': curl_error,
              'req_total': float(queries_total),
              'time_elapsed': time_elapsed,
              '__response_times': response_times}

    if len(response_times) > 1:
        result['qps'] = float(queries_total) / time_elapsed
        result['req_min'] = min(response_times)
        result['req_max'] = max(response_times)
        result['req_mean'] = statistics.mean(response_times)
        result['req_median'] = statistics.median(response_times)
        result['req_stdev'] = statistics.stdev(response_times)

    return result


def print_results(result_dict):
    """Print results in pretty format.
    """

    print()
    print('Finished test in %.3f seconds' % (result_dict['time_elapsed'],))
    print('Total requests done: %s' % (result_dict['req_total'],))
    if 'qps' in result_dict:
        print('Response rate: %.3f qps' % (result_dict['qps'],))
    print('HTTP OK responses: %s' % (result_dict['http_ok'],))
    print('HTTP error responses: %s' % (result_dict['http_error'],))
    print('Curl errors: %s' % (result_dict['curl_error'],))

    if 'req_min' in result_dict:
        print('Minimum success response time: %s ms' %
              (result_dict['req_min'],))

    if 'req_max' in result_dict:
        print('Maximum success response time: %s ms' %
              (result_dict['req_max'],))

    if 'req_mean' in result_dict:
        print('Success response time mean: %.3f ms' %
              (result_dict['req_mean'],))

    if 'req_median' in result_dict:
        print('Success response time median: %.3f ms' %
              (result_dict['req_median'],))

    if 'req_stdev' in result_dict:
        print('Success response time standard deviation: %.3f ms' %
              (result_dict['req_stdev'],))


def run_engine(url, num_connections, num_requests, timeout, keep_alive):
    """Run HTTPQueryEngine instance.

    :param url: server URL.
    :param num_connections: number simultaneous connections per process.
    :param num_requests: number of requests.
    :param timeout: connection timeout.
    :param keep_alive: whether to keep connection opened.

    :return: summary dict.
    """

    with HTTPQueryEngine(url, num_connections=num_connections,
                         keep_alive_conn=keep_alive, timeout=timeout) as query:
        return query(num_requests=num_requests)


def parse_args():
    """Parse command line arguments.

    :rtype: argparse.Namespace
    :return: command line arguments object.
    """

    parser = argparse.ArgumentParser(
        description='HTTP server benchmark application.')
    parser.add_argument(
        '-c', '--num-connections', metavar='num_connections', type=int,
        help='The number of simultaneous connections per worker process. '
             'Default: %s.' %
             (DEF_NUM_CONNECTIONS,), default=DEF_NUM_CONNECTIONS)
    parser.add_argument(
        '-p', '--num-processes', metavar='num_processes', type=int,
        help='The number of worker processes. '
             'Default: 1.', default=1)
    parser.add_argument(
        '-n', '--num-requests', metavar='num_requests', type=int,
        help='Total number of HTTP requests. Default: infinity.',
        default=INFINITY)
    parser.add_argument(
        '-t', '--timeout', metavar='timeout', type=int,
        help='Connection timeout. Default: %s.' % (DEF_CONNECTION_TIMEOUT,),
        default=DEF_CONNECTION_TIMEOUT)
    parser.add_argument('-v', '--verbose', help='Verbose mode.',
                        default=False, action='store_true')
    parser.add_argument('-k', '--keep-alive', metavar='keep_alive',
                        help='Whether to keep connection alive.',
                        default=1, type=int, action='store')
    parser.add_argument(
        '-u', '--url', metavar='url', type=str, required=True,
        help='Full API server UTL e.g. http://127.0.0.1:8000/ping.')

    return parser.parse_args()


def main():
    """Entry point.
    """

    cmd_args = parse_args()

    log_level = logging.INFO
    if cmd_args.verbose:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    futures = []

    if cmd_args.num_processes > 1:
        logging.getLogger('query_http_server').info(
            'Running pool of %s simultaneous processes', cmd_args.num_processes)

        try:
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=cmd_args.num_processes) as executor:

                for _ in range(cmd_args.num_processes):
                    futures.append(executor.submit(run_engine, cmd_args.url,
                                                   cmd_args.num_connections,
                                                   cmd_args.num_requests,
                                                   cmd_args.timeout,
                                                   bool(cmd_args.keep_alive)))
        except KeyboardInterrupt:
            logging.getLogger(
                'query_http_server').info('Master process got SIGINT signal.')

        future_results = []
        for future in futures:
            future_results.append(future.result())

        result_dict = _aggregate_worker_stats(future_results)
    else:
        result_dict = run_engine(cmd_args.url, cmd_args.num_connections,
                                 cmd_args.num_requests, cmd_args.timeout,
                                 bool(cmd_args.keep_alive))

    print_results(result_dict)


if __name__ == '__main__':

    main()
