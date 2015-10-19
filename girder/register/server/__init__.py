from girder import events
from girder.utility.model_importer import ModelImporter

def user_saved(event):
    if not event.info['groups'] and '_id' in event.info:
        query = {
            'name': 'hydra-th-members'
        }
        group = ModelImporter.model('group').findOne(query=query)

        if group:
            ModelImporter.model('group').addUser(group, event.info)

def load(info):
    events.bind('model.user.save.after', 'test', user_saved)

