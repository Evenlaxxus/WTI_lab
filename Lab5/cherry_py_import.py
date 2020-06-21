import cherrypy

from app import app

if __name__ == '__main__':
    cherrypy.tree.graft(app.wsgi_app, '/')
    cherrypy.config.update({
        'server.socket_host': '127.0.0.1',
        'server.socket_port': 9898,
        'engine.autoreload.on': False
    })

    cherrypy.engine.start()
