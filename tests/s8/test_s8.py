from fastapi.testclient import TestClient


class TestS8Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """


    def test_aircraft8(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft")
            assert not response.is_error, "No data found in db"

    def test_co2_1(self, client: TestClient) -> None:
        icao = "no_icao"
        day="2024-10-01"
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
            assert response.is_error, "No records found"
            assert response.status_code == 404

    def test_co2_2(self, client: TestClient) -> None:
        icao = "a7f3ab"
        day="20241001"
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
            assert response.is_error, "Something is wrong with the request"
            assert response.status_code == 422


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    def test_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft")
            assert not response.is_error, "Error at the aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type", "owner", "manufacturer", "model"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_co2(self, client: TestClient) -> None:
        icao = "a7f3ab"
        day="2024-10-01"
        with client as client:
            response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
            assert not response.is_error, "Error at the positions endpoint"
            r = response.json()
            for field in ["icao", "hours_flown", "co2"]:
                assert field in r, f"Missing '{field}' field."
