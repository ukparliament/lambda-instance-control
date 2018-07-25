import os.path as op
import re
import unittest
from .fakes import FakeRequests

class TestFakeRequests(unittest.TestCase):

    def test_get(self):
        fake_requests = FakeRequests()
        fake_requests.add_response(
            '/test',
            """
                {
                    "Hello": "world"
                }
            """)
        data = fake_requests.get('/test').json()
        self.assertDictEqual({"Hello": "world"}, data)

    def test_get_from_file(self):
        fake_requests = FakeRequests()
        fake_requests.add_response(
            '/test',
            content_file=op.join(op.dirname(__file__), 'data/pingdom/checks.json'),
            header_file=op.join(op.dirname(__file__), 'data/pingdom/checks.headers.txt')
        )
        resp = fake_requests.get('/test')
        self.assertIn('checks', resp.json())
        self.assertIn('Content-Type', resp.headers)

    def test_get_regex(self):
        fake_requests = FakeRequests()
        fake_requests.add_response(
            re.compile('^/hello'),
            """
            {
                "Hello": "world"
            }
            """
        )
        data = fake_requests.get('/hello').json()
        self.assertDictEqual({"Hello": "world"}, data)

    def test_get_regex2(self):
        mydir = op.abspath(op.dirname(__file__))
        datadir = op.join(mydir, 'data/pingdom')
        r = FakeRequests()
        r.add_response(
            re.compile('summary.outage'),
            content_file=op.join(datadir, 'summary.outage.3603211.json'),
            header_file=op.join(datadir, 'summary.outage.3603211.headers.txt')
        )
        r.add_response(
            re.compile('checks'),
            content_file=op.join(datadir, 'checks.json'),
            header_file=op.join(datadir, 'checks.headers.txt')
        )

        data = r.get('/checks').json()
        self.assertTrue(data)
        self.assertIn('checks', data)
