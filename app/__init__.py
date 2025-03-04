from flask import Flask
from .config import Config
from .utils import init_db
from .routes import api_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize database connections with pools
    init_db(app)

    # Register blueprints (routes)
    app.register_blueprint(api_bp, url_prefix="/api")

    return app