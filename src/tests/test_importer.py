from datetime import datetime
import os.path
import re
import unittest
import unittest.mock

import factoryfactory
import requests
import credstash

from .fakes import FakeRequests
from targets import influx
import importers


class PingdomOutageHarness:

    def __init__(self):
        self.mydir = os.path.abspath(os.path.dirname(__file__))
        self.datadir = os.path.join(self.mydir, 'data/pingdom')
        self.services = factoryfactory.ServiceLocator()
        self.services.register(credstash, unittest.mock.Mock, singleton=True)


    # ====== Arrange ====== #

    def register_fake_pingdom_requests(self):
        r = FakeRequests()
        r.add_response(
            re.compile(r'summary\.outage.*3603211'),
            content_file=os.path.join(self.datadir, 'summary.outage.3603211.json'),
            header_file=os.path.join(self.datadir, 'summary.outage.3603211.headers.txt')
        )
        r.add_response(
            re.compile('checks'),
            content_file=os.path.join(self.datadir, 'checks.json'),
            header_file=os.path.join(self.datadir, 'checks.headers.txt')
        )
        r.add_response(None, text="{}", headers={})
        self.fake_requests = r
        self.services.register(requests, r)

    def register_empty_mock_influx_repository(self):
        """
        Creates a mock InfluxDB repository, which will return an empty result
        set, indicating that there are no previously existent data points.
        """
        mock = unittest.mock.Mock()
        mock.get_last_endpoint_data.return_value = { }
        self.mock_influx_repository = mock
        self.services.register(
            influx.InfluxRepository,
            lambda *args, **kwargs: self.mock_influx_repository,
            singleton=True
        )

    def register_mock_influx_repository(self, endpoint_id, start_time, status, duration):
        """
        Creates a mock InfluxDB repository, which will return a most recent
        entry with the specified start and end times and status.

        :param endpoint_id:
            The numeric endpoint ID.
        :type endpoint_id: int
        :param start_time:
            The start time of the last entry in the sequence.
        :type start_time: datetime
        :param status:
            The status of the last entry in the sequence.
        :type status: str
        :param duration:
            The duration of the last entry in the sequence, in seconds.
        :type duration: int
        :return:
            A mock InfluxDB repository.
        """
        mock = unittest.mock.Mock()
        mock.get_last_endpoint_data.return_value = {
            endpoint_id: {
                'time': start_time,
                'status': status,
                'duration': duration
            }
        }
        self.mock_influx_repository = mock
        self.services.register(
            influx.InfluxRepository,
            lambda *args, **kwargs: self.mock_influx_repository,
            singleton=True
        )


    # ====== Act ====== #

    def execute(self):
        importer = self.services.get(importers.PingdomImporter)
        importer.import_outages()


class Test_PingdomOutageImporter(unittest.TestCase):
    """
    Tests to confirm that we can import the data from Pingdom correctly.

    The outages returned from Pingdom only cover the specified time window,
    which by default is one week. The first entry returned will have the same
    start time as the start of the window, while the last entry returned will
    have the same end time as the end of the window.

    This means that any outage returned by the API may correspond to an entry
    that already exists within InfluxDB, but either its start time, or its
    end time, or both, may differ.

    The key here is to fetch the last entry from InfluxDB and compare it
    against the outages returned from Pingdom:

     * An outage that ended before the start time of the last entry can be
       assumed to exist in the database already, and should be discarded,
       with an INFO log status.
     * An outage that started after the end time of the last entry can be
       assumed to be a new entry, and should be inserted into the database
       with an INFO log status.
     * Outages that overlap with the last entry should be treated as follows:
       * Any outages whose statuses are different from the last entry should
         be discarded with a WARN log status.
       * Any outages whose end times are earlier than the end time of the
         last entry should be discarded with a WARN log status.
       * If there is more than one overlapping outage with a matching status,
         only the last one should be considered, and the others should be
         discarded with a WARN log status.
       * Only the end time of the last entry should be updated. If the start
         time of the outage is earlier than the start time of the last entry,
         a WARN log status should be recorded. Otherwise, an INFO log status
         should be recorded.
    """

    def assert_imported(self, harness, count=3, first_time=None, first_duration=None):
        """
        Checks that we've imported all the expected outages.

        :param harness:
            The test harness object.
        :param count:
            The number of data points that we expect to have imported.
            Only the last n data points will be considered: the first of them
            will be discarded.
        :param first_time:
            The Unix timestamp of the first expected time.
        :param first_duration:
            The Unix timestamp of the first expected duration.
        """

        method = harness.mock_influx_repository.write_data
        self.assertEqual(1, method.call_count)

        args, kwargs = method.call_args_list[0]

        # Check that we're saving it to "outages" and that the tags are correct

        self.assertEqual('outages', args[0])
        self.assertDictEqual({
            'endpoint': 'Current DDP',
            'hostname': 'lda.data.parliament.uk',
            'endpoint_id': 3603211
        }, args[2])

        # Now check that the times being inserted are correct

        expected_times = [1520956105, 1521032518, 1521032638]
        expected_data_points = [
            {'status': 'up', 'duration': 1521032518 - 1520956105},
            {'status': 'down', 'duration': 1521032638 - 1521032518},
            {'status': 'up', 'duration': 1521560878 - 1521032638},
        ]
        if count <= 3 and count >= 0:
            expected_times = expected_times[3-count:]
            expected_data_points = expected_data_points[3-count:]
        else:
            raise ValueError('count must be between 0 and 3')

        if count:
            if first_time:
                expected_times[0] = first_time
            if first_duration:
                expected_data_points[0]['duration'] = first_duration

        times = [int(t.time.timestamp()) for t in args[1]]
        self.assertListEqual(expected_times, times)

        # Also check that the statuses being inserted are correct
        data_points = [t.values for t in args[1]]
        self.assertListEqual(expected_data_points, data_points)


    def assert_all_imported_as_is(self, harness):
        """
        Convenience method to make it explicit that we're importing everything.
        This is the default for assert_imported.

        :param harness:
            The test harness.
        """
        self.assert_imported(harness)


    # ====== Tests ====== #

    def test_import_initial_outages(self):
        """
        Tests to make sure that we can import the outages when there are no
        previously existent data points.
        """

        # Arrange
        harness = PingdomOutageHarness()
        harness.register_empty_mock_influx_repository()
        harness.register_fake_pingdom_requests()

        # Act
        harness.execute()

        # Assert
        self.assert_all_imported_as_is(harness)

    def test_nonoverlapping_outages(self):
        """
        Tests that all outages are imported correctly when the last existent
        data point is before the current series.
        """

        # Arrange
        harness = PingdomOutageHarness()
        harness.register_mock_influx_repository(
            endpoint_id=3603211,
            start_time=datetime.fromtimestamp(1520000000),
            status='down',
            duration=956105
        )
        harness.register_fake_pingdom_requests()

        # Act
        harness.execute()

        # Assert
        self.assert_all_imported_as_is(harness)

    def test_first_overlap(self):
        """
        Tests that all outages are imported correctly when the last existent
        data point overlaps the first current one. That is, the old last time is
        the same as the new first time.
        """

        # Arrange
        harness = PingdomOutageHarness()
        harness.register_mock_influx_repository(
            endpoint_id=3603211,
            start_time=datetime.fromtimestamp(1520000000),
            status='up',
            duration=1000000
        )
        harness.register_fake_pingdom_requests()

        # Act
        harness.execute()

        # Assert
        self.assert_imported(harness, first_time=1520000000, first_duration=1032518)

    def test_second_overlap(self):
        """
        Tests that all outages are imported correctly when the last existent
        data point overlaps the second current one. That
        """

        # Arrange
        harness = PingdomOutageHarness()
        harness.register_mock_influx_repository(
            endpoint_id=3603211,
            start_time=datetime.fromtimestamp(1521000000),
            status='down',
            duration=32550
        )
        harness.register_fake_pingdom_requests()

        # Act
        harness.execute()

        # Assert
        self.assert_imported(harness, count=2, first_time=1521000000, first_duration=32638)

    def test_second_overlap_different(self):
        """
        Tests that all outages are imported correctly when the last existent
        data point overlaps the second current one. That
        """

        # Arrange
        harness = PingdomOutageHarness()
        harness.register_mock_influx_repository(
            endpoint_id=3603211,
            start_time=datetime.fromtimestamp(1521000000),
            status='up',
            duration=32550
        )
        harness.register_fake_pingdom_requests()

        # Act
        harness.execute()

        # Assert
        self.assert_imported(harness, count=2)



# ====== Conflation tests ====== #

class Test_Conflate_DataPoints(unittest.TestCase):

    def test_non_overlapping(self):
        old_time = datetime.fromtimestamp(1000)
        new_time = datetime.fromtimestamp(2000)
        old_data = {'status': 'down', 'duration': 500}
        new_data = {'status': 'up', 'duration': 1000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertEqual(new_time, time)
        self.assertDictEqual(new_data, data)

    def test_touching_different(self):
        old_time = datetime.fromtimestamp(1000)
        new_time = datetime.fromtimestamp(2000)
        old_data = {'status': 'down', 'duration': 1000}
        new_data = {'status': 'up', 'duration': 1000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertEqual(new_time, time)
        self.assertDictEqual(new_data, data)

    def test_touching_same(self):
        old_time = datetime.fromtimestamp(1000)
        new_time = datetime.fromtimestamp(2000)
        old_data = {'status': 'up', 'duration': 1000}
        new_data = {'status': 'up', 'duration': 1000}
        expected_data = {'status': 'up', 'duration': 2000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertEqual(old_time, time)
        self.assertDictEqual(expected_data, data)


    def test_late_overlap_same(self):
        old_time = datetime.fromtimestamp(1000)
        new_time = datetime.fromtimestamp(2000)
        old_data = {'status': 'up', 'duration': 1500}
        new_data = {'status': 'up', 'duration': 1000}
        expected_data = {'status': 'up', 'duration': 2000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertEqual(old_time, time)
        self.assertDictEqual(expected_data, data)

    def test_late_overlap_different(self):
        old_time = datetime.fromtimestamp(1000)
        new_time = datetime.fromtimestamp(2000)
        old_data = {'status': 'up', 'duration': 1500}
        new_data = {'status': 'down', 'duration': 1000}
        expected_data = {'status': 'down', 'duration': 1000 }
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertEqual(new_time, time)
        self.assertDictEqual(expected_data, data)

    def test_early_overlap_same(self):
        new_time = datetime.fromtimestamp(1000)
        old_time = datetime.fromtimestamp(2000)
        new_data = {'status': 'up', 'duration': 1500}
        old_data = {'status': 'up', 'duration': 1000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertFalse(time)
        self.assertFalse(data)

    def test_early_overlap_different(self):
        new_time = datetime.fromtimestamp(1000)
        old_time = datetime.fromtimestamp(2000)
        new_data = {'status': 'down', 'duration': 1500}
        old_data = {'status': 'up', 'duration': 1000}
        time, data = importers.conflate_datapoints(old_time, old_data, new_time, new_data)
        self.assertFalse(time)
        self.assertFalse(data)

