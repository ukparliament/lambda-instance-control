from unittest.mock import Mock
import requests

class FakeRequests(object):
    """
    An object to fake the requests library.
    """

    def __init__(self):
        self.responses = {}
        self.default_response = None

    def add_response(self, url, text=None, headers=None, status_code=200,
        exception=None, content_file=None, content_stream=None,
        header_file=None):
        """
        Sets up the response. This will create an actual Response object from
        the requests library so it can be used as expected.

        :param url:
            The URL or URL pattern to match. This can be either a string
            (for an exact match) or a regular expression object (for a pattern
            match), or None (for the default response if no other has been found).
        :param text:
            The text content to return from the request.
        :param headers:
            The HTTP headers to return from the request.
        :param status_code:
            The status code to return from the request.
        :param exception:
            An exception to throw instead of returning a response.
        :param content_file:
            Path to a file containing the content to return from the request.
        :param content_stream:
            A stream-like object containing the content to return from the request.
        :param header_file:
            Path to a file containing the headers to return from the request.
        """
        from requests.models import Response
        from requests.structures import CaseInsensitiveDict
        response = Response()
        if text:
            response._content = text.encode('utf-8')
            response.encoding = "utf-8"
        elif content_file:
            response.raw = open(content_file, 'rb')
            response.request = Mock(stream=True)
        elif content_stream:
            response.raw = content_stream
            response.request = Mock(stream=True)
        if headers:
            response.headers = CaseInsensitiveDict(headers)
        elif header_file:
            with open(header_file) as f:
                response.headers = CaseInsensitiveDict([
                    [s.strip() for s in line.split(':', 1)]
                    for line in f
                ])
        response.status_code = status_code
        if not url:
            self.default_response = (response, exception)
        else:
            if url in self.responses:
                response_list = self.responses[url]
            else:
                self.responses[url] = response_list = []
            response_list.append((response, exception))

    def request(self, url, *args, **kwargs):
        if not isinstance(url, str):
            raise ValueError('url needs to be a string')
        response_list = self.responses.get(url)
        if not response_list:
            candidates = [
                k for k in self.responses
                if hasattr(k, 'search')
                and k.search(url)
            ]
            if not candidates:
                response_list = []
            else:
                response_list = self.responses[candidates[0]]
        if len(response_list) == 0:
            if self.default_response:
                response, exception = self.default_response
            else:
                raise ValueError('No responses remaining for the URL ' + url)
        elif len(response_list) == 1:
            response, exception = response_list[0]
        else:
            response, exception = response_list.pop()
        if exception:
            raise exception
        return response

    def get(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def head(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def post(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def patch(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def put(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def delete(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)

    def options(self, url, *args, **kwargs):
        return self.request(url, *args, **kwargs)
