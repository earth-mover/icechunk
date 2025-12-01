import socket
import threading
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, cast

import icechunk as ic
import xarray as xr

TARGET_URL = "s3://icechunk-public-data/v1/era5_weatherbench2?region=us-east-1"


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return cast(int, s.getsockname()[1])


class RedirectHandler(BaseHTTPRequestHandler):
    """Handles GET requests by sending 302 redirects"""

    def do_GET(self) -> None:
        req_num = int(self.path[1:] or 0)
        # redirect to ourselves a few times until we finally redirect to s3://
        location = TARGET_URL if req_num >= 6 else f"/{req_num + 1}"
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        pass


def run_server(port: int) -> tuple[HTTPServer, threading.Thread]:
    """Run a server that redirects to itself a few times and ends up redirecting to TARGET_URL"""
    server_address = ("", port)
    server = HTTPServer(server_address, RedirectHandler)
    thread = threading.Thread(None, server.serve_forever)
    thread.start()
    return (server, thread)


def test_era5_with_redirect() -> None:
    """Starts an http server that returns a few redirects to itself and finally redirects
    to the s3 address of a public dataset. Then it creates a redirect_storage instance
    pointing to that server. Finally it verifies it can open the dataset."""

    try:
        port = find_free_port()
        (server, thread) = run_server(port)
        storage = ic.redirect_storage(f"http://localhost:{port}")
        repo = ic.Repository.open(storage=storage)

        # redirect server is no longer needed at this point
        server.shutdown()

        session = repo.readonly_session("main")
        ds = xr.open_dataset(
            session.store,  # type: ignore[arg-type]
            group="1x721x1440",
            engine="zarr",
            chunks=None,
            consolidated=False,
        )
        assert ds is not None

    finally:
        server.shutdown()
        thread.join()
