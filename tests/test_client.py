"""pact test for user service client"""

import json
import logging
import os

import pytest
import requests

from pact_python_demo.client import UserClient
from pact import Consumer, Like, Provider

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PACT_UPLOAD_URL = (
    "https://lacework.pactflow.io/pacts/provider/UserService/consumer/UserServiceClient/version"
)

PACT_FILE = "userserviceclient-userservice.json"

PACT_MOCK_HOST = 'localhost'
PACT_MOCK_PORT = 1234
PACT_DIR = os.path.dirname(os.path.realpath(__file__))+'//pact'
CONSUMER = 'UserServiceClient'
PROVIDER = 'UserService'


@pytest.fixture
def client():
    return UserClient(
        'http://{host}:{port}'
            .format(host=PACT_MOCK_HOST, port=PACT_MOCK_PORT)
    )


def push_to_broker(version):
    with open(os.path.join(PACT_DIR, PACT_FILE), 'rb') as pact_file:
        pact_file_json = json.load(pact_file)
    headers = {'Authorization': 'Bearer AkSlvSV8MLjzoIDe6AhoqQ'}
    log.info("Uploading pact file to pact broker...")

    r = requests.put("{}/{}".format(PACT_UPLOAD_URL, version),headers=headers,json=pact_file_json)
    log.info("Uploaded to pact broker : {}/{}".format(PACT_UPLOAD_URL, version))
    if not r.ok:
        log.error("Error uploading: %s", r.content)
        r.raise_for_status()


@pytest.fixture(scope='session')
def pact(request):
    pact = Consumer(CONSUMER).has_pact_with(
        Provider(PROVIDER), host_name=PACT_MOCK_HOST, port=PACT_MOCK_PORT,
        pact_dir=PACT_DIR)
    pact.start_service()
    yield pact
    pact.stop_service()

    version = request.config.getoption('--publish-pact')
    if not request.node.testsfailed and version:
        push_to_broker(version)


def test_get_user_non_admin(pact, client):
    expected = {
        'name': 'UserA',
        'id': '123456',
        'admin': False
    }

    (pact
     .given('UserA exists and is not an administrator')
     .upon_receiving('a request for UserA')
     .with_request('get', '/users/UserA')
     .will_respond_with(200, body=Like(expected)))

    with pact:
        result = client.get_user('UserA')
        assert result is not None

    # assert something with the result, for ex, did I process 'result' properly?
    # or was I able to deserialize correctly? etc.


def test_get_non_existing_user(pact, client):
    (pact
     .given('UserA does not exist')
     .upon_receiving('a request for UserA')
     .with_request('get', '/users/UserA')
     .will_respond_with(404))

    with pact:
        result = client.get_user('UserA')

    assert result is None
