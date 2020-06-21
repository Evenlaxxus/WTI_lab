import cherrypy
from flask import json
from pandas.io.json import json_normalize

from controller import Controller


@cherrypy.expose
@cherrypy.tools.json_out()
class GetRatings(object):
    @cherrypy.tools.accept(media='application/json')
    def GET(self):
        return controller.data.fillna(0).to_json(orient="records")


@cherrypy.expose
@cherrypy.tools.json_out()
class PostRating(object):
    @cherrypy.tools.accept(media='application/json')
    def POST(self):
        cl = cherrypy.request.headers['Content-Length']
        body = cherrypy.request.body.read(int(cl))
        result = json.loads(body)
        row = json_normalize(result)
        controller.addRow(row),
        return result

    @cherrypy.tools.accept(media='application/json')
    def DELETE(self):
        cl = cherrypy.request.headers['Content-Length']
        body = cherrypy.request.body.read(int(cl))
        request_data = json.loads(body)
        controller.deleteRow(request_data)
        return request_data


@cherrypy.expose
@cherrypy.tools.json_out()
class AvgRatingsAllUsers(object):
    @cherrypy.tools.accept(media='application/json')
    def GET(self):
        return controller.genre_ratings()


@cherrypy.expose
@cherrypy.popargs('userID')
@cherrypy.tools.json_out()
class AvgRatingsUser(object):
    @cherrypy.tools.accept(media='application/json')
    def GET(self, userID):
        return controller.genre_user_ratings(userID), {'Content-Type': 'application/json'}


if __name__ == '__main__':
    controller = Controller()

    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            "request.methods_with_bodies": ("POST", "PUT", "DELETE"),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
        'global': {
            'engine.autoreload.on': False
        }
    }

    cherrypy.config.update({'server.socket_port': 9898})
    cherrypy.tree.mount(GetRatings(), '/ratings', conf)
    cherrypy.tree.mount(PostRating(), '/rating', conf)
    cherrypy.tree.mount(AvgRatingsAllUsers(), '/avg-genre-ratings/all-users', conf)
    cherrypy.tree.mount(AvgRatingsUser(), '/avg-genre-ratings', conf)

    cherrypy.engine.start()
    cherrypy.engine.block()
