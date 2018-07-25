import os.path
import unittest
import unittest.mock

import credstash
import factoryfactory
import requests

from sources import pingdom
from . import fakes

class Test_Pingdom(unittest.TestCase):

    def test_get_results(self):
        services = factoryfactory.ServiceLocator()

        fakeRequests = fakes.FakeRequests()
        datadir = os.path.join(os.path.dirname(__file__), 'data/pingdom')
        fakeRequests.add_response(
            url=pingdom.PINGDOM_BASE_URL + 'results/3603211',
            content_file=os.path.join(datadir, 'results.3603211.json'),
            header_file=os.path.join(datadir, 'results.3603211.headers.txt')
        )
        services.register(requests, fakeRequests)
        services.register(credstash, unittest.mock.Mock())

        pinger = services.get(pingdom.PingdomReader)
        results = list(pinger.get_results(check_id=3603211))
        self.assertEqual(1001, len(results))