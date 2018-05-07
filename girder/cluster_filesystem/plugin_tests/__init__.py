import pytest

import girder.events


@pytest.fixture
def unbound_server(server):
    yield server

    events = [
        ('rest.get.folder.before', 'newt_folders'),
        ('rest.get.folder/:id.before', 'newt_folders'),
        ('rest.get.item.before', 'newt_folders'),
        ('rest.get.item/:id.before', 'newt_folders'),
        ('rest.get.item/:id/files.before', 'new_folders'),
        ('rest.get.file/:id.before', 'newt_folders'),
        ('rest.get.file/:id/download.before', 'newt_folders')
    ]

    for event_name, handler_name in events:
        girder.events.unbind(event_name, handler_name)