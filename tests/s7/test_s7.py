from fastapi.testclient import TestClient


class TestS7Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_prepare7(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert not response.is_error, "No files found in S3."

    def test_aircraft7(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/aircraft")
            assert not response.is_error, "No data found in files."

    def test_positions7(self, client: TestClient) -> None:
        icao = "no_icao"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/positions")
            r = response.json()
            assert len(r) == 0, "icao does not exist"

    def test_stats7(self, client: TestClient) -> None:
        icao = "no_icao"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            r = response.json()
            assert r['had_emergency'] is False, "icao does not exist"
            assert r['max_altitude_baro'] is None, "icao does not exist"
            assert r['max_ground_speed'] is None, "icao does not exist"


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_prepare(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s7/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_positions(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/positions")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["timestamp", "lat", "lon"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_stats(self, client: TestClient) -> None:
        icao = "06a0af"
        with client as client:
            response = client.get(f"/api/s7/aircraft/{icao}/stats")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["max_altitude_baro", "max_ground_speed", "had_emergency"]:
                assert field in r, f"Missing '{field}' field."
