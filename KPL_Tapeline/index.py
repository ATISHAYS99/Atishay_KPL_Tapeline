from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS
from Tapeline.Routes.index_x import routes

def create_app():
    app = Flask(__name__)

    # Enable Cross-Origin Resource Sharing (CORS)
    CORS(app)

    # Register the routes blueprint with a URL prefix
    app.register_blueprint(routes, url_prefix="/app/v1")

    return app