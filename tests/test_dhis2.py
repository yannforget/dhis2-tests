import httpx


def test_connect():
    auth = httpx.BasicAuth("admin", "district")
    client = httpx.Client(timeout=30, auth=auth)
    r = client.get("http://localhost:8080/api/system/info")
    assert r.status_code == 200
