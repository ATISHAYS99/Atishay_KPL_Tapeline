# Import the 'application' function from the 'index' module of the 'v1' package

from index import create_app

app = create_app()

app.run(host = '0.0.0.0', port=5001)