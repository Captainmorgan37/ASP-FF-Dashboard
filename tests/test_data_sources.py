import unittest

from data_sources import FL3XX_SCHEDULE_COLUMNS, load_schedule


class Fl3xxApiLoaderTests(unittest.TestCase):
    def test_transforms_flights_into_expected_dataframe(self):
        flights = [
            {
                "bookingIdentifier": "EBVAO",
                "accountName": "Richard Pilosof",
                "registrationNumber": "C-GFSD",
                "aircraftCategory": "C25B",
                "airportFrom": "CYUL",
                "airportTo": "CYYZ",
                "blockOffEstUTC": "2025-10-02T01:00:00.000Z",
                "blockOnEstUTC": "2025-10-02T02:17:00.000Z",
                "workflowCustomName": "FEX As Available",
            }
        ]

        data = load_schedule("fl3xx_api", metadata={"flights": flights})
        frame = data.frame

        self.assertListEqual(list(frame.columns), FL3XX_SCHEDULE_COLUMNS)
        self.assertEqual(len(frame), 1)

        row = frame.iloc[0]
        self.assertEqual(row["Booking"], "EBVAO")
        self.assertEqual(row["Account"], "Richard Pilosof")
        self.assertEqual(row["Aircraft"], "C-GFSD")
        self.assertEqual(row["Aircraft Type"], "C25B")
        self.assertEqual(row["From (ICAO)"], "CYUL")
        self.assertEqual(row["To (ICAO)"], "CYYZ")
        self.assertEqual(row["Workflow"], "FEX As Available")
        self.assertEqual(row["Off-Block (Sched)"], "02.10.2025 01:00")
        self.assertEqual(row["On-Block (Sched)"], "02.10.2025 02:17")
        self.assertEqual(row["Flight time (Est)"], "01:17")
        self.assertEqual(row["PIC"], "")
        self.assertEqual(row["SIC"], "")

    def test_handles_missing_or_invalid_fields(self):
        flights = [
            {
                "bookingReference": 12345,
                "airportFrom": None,
                "airportTo": "",
                "blockOffEstUTC": None,
                "blockOnEstUTC": None,
            }
        ]

        data = load_schedule("fl3xx_api", metadata={"flights": flights})
        frame = data.frame

        self.assertListEqual(list(frame.columns), FL3XX_SCHEDULE_COLUMNS)
        self.assertEqual(len(frame), 1)

        row = frame.iloc[0]
        self.assertEqual(row["Booking"], "12345")
        self.assertEqual(row["From (ICAO)"], "")
        self.assertEqual(row["To (ICAO)"], "")
        self.assertEqual(row["Off-Block (Sched)"], "")
        self.assertEqual(row["On-Block (Sched)"], "")
        self.assertEqual(row["Flight time (Est)"], "")


if __name__ == "__main__":
    unittest.main()
