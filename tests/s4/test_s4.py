from fastapi.testclient import TestClient


class TestS4Student:
    """
    As you code it's always important to ensure that your code reflects
    the business requisites you have.
    The optimal way to do so is via tests.
    Use this class to create functions to test your application.

    For more information on the library used, search `pytest` in your preferred search engine.
    """

    def test_download1(self, client: TestClient) -> None:
        with client as client:
            response = client.post("/api/s4/aircraft/download?file_limit=0")
            assert response.is_error, "Limit must be greater than 0"
            assert response.status_code == 400

    #def test_download2(self, client: TestClient) -> None:
    #    with client as client:
    #        response = client.post("/api/s4/aircraft/download?file_limit=1")
    #        assert not response.is_error, "Failed to fetch file URLs"

    #def test_prepare1(self, client: TestClient) -> None:
    #    with client as client:
    #        response = client.post("/api/s4/aircraft/prepare")
    #        assert not response.is_error, "No files found in S3."



class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `poetry run pytest` or it will be a 0!
    """

    #def test_download(self, client: TestClient) -> None:
    #    with client as client:
    #        response = client.post("/api/s4/aircraft/download?file_limit=1")
    #        assert not response.is_error, "Error at the download endpoint"


    #def test_prepare(self, client: TestClient) -> None:
    #    with client as client:
    #        response = client.post("/api/s4/aircraft/prepare")
    #        assert not response.is_error, "Error at the prepare endpoint"



